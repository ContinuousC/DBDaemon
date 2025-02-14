/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::path::Path;
use std::str::FromStr;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use clap::Args;
use futures::TryFutureExt;
use http::Method;
use log::{debug, info};
use reqwest::{Body, Certificate, Client, Identity, RequestBuilder, Response, StatusCode, Url};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::fs;

use dbschema::{DbSchema, DbTable, DbTableId, Filter};
use uuid::Uuid;

use dbschema_elastic::{ElasticFilter, ElasticMapping, ElasticValue};

use crate::database::backend::Database as DatabaseTrait;

use super::bulk_op::BulkOp;
use super::error::{Error, InitializationError, Result};
use super::requests::{CreateIndex, IndexSettings, Pit, SearchRequest};
use super::responses::{
    BulkReponse, ClusterDistribution, ClusterInfoResponse, DocumentResponse, IndexResponse,
    PitResponse, QueryResponse, RefreshResponse, UpdateByQueryResponse,
};

#[derive(Debug)]
pub struct Database {
    config: DatabaseConfig,
    client: Client,
    pub base_url: Url,
    opensearch: AtomicBool,
}

#[derive(Serialize, Deserialize, Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug)]
#[serde(transparent)]
pub struct ElasticId(String);

impl ElasticId {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }
}

impl Database {
    pub async fn from_config(config_dir: &Path) -> Result<Self> {
        let config_file = config_dir.join("elastic.yaml");
        Self::new(
            serde_yaml::from_str(
                &fs::read_to_string(config_file.as_path())
                    .await
                    .map_err(|e| InitializationError::ReadConfigIo(config_file.clone(), e))?,
            )
            .map_err(|e| InitializationError::ReadConfigFmt(config_file.clone(), e))?,
        )
        .await
    }

    pub async fn new(config: DatabaseConfig) -> Result<Database> {
        let mut url = Url::parse(&config.url).map_err(InitializationError::InvalidUrl)?;
        if url.port().is_none() {
            let _ = url.set_port(Some(9200));
        }

        // Note: sidev-evn0 address hardcoded because SI dev server
        // sometimes returns errors.

        let mut client = Client::builder().resolve(
            "sidev-evn0",
            SocketAddr::from_str("192.168.2.85:9200").unwrap(),
        );

        if let Some(path) = &config.ca {
            let cert = Certificate::from_pem(
                fs::read(path)
                    .await
                    .map_err(|e| InitializationError::ReadCa(path.to_string(), e))?
                    .as_slice(),
            )
            .map_err(|e| InitializationError::ParseCa(path.to_string(), e))?;
            client = client.tls_built_in_root_certs(false);
            client = client.add_root_certificate(cert);
        }

        match (&config.cert, &config.key) {
            (Some(cert), Some(key)) => {
                let cert = fs::read(cert)
                    .await
                    .map_err(|e| InitializationError::ReadClientCert(cert.to_string(), e))?;
                let key = fs::read(key)
                    .await
                    .map_err(|e| InitializationError::ReadClientKey(key.to_string(), e))?;
                client = client.identity(
                    Identity::from_pkcs8_pem(&cert, &key)
                        .map_err(InitializationError::ParseClientCert)?,
                );
            }
            (Some(_), None) | (None, Some(_)) => {
                log::warn!("To use elasticsearch client certificate, you need to specify both --elastic-cert and --elastic-key");
            }
            (None, None) => {}
        }

        Ok(Database {
            config,
            client: client.build().map_err(InitializationError::BuildClient)?,
            base_url: url,
            opensearch: AtomicBool::new(false),
        })
    }

    pub fn load(schema: &DbSchema, db_output: ElasticValue) -> Result<Value> {
        Ok(db_output.load(schema)?)
    }
    // convert a json-value to the correponding database-value
    pub fn save(schema: &DbSchema, db_input: Value) -> Result<ElasticValue> {
        Ok(ElasticValue::save(schema, db_input)?)
    }

    /// Calculate elasticsearch index name.
    pub fn get_index_name(&self, table_id: &DbTableId) -> String {
        let prefix = &self.config.index_prefix;
        format!("{prefix}-{table_id}")
    }

    async fn refresh_index(&self, index: &str) -> Result<()> {
        let _ = self
            .get::<RefreshResponse>(&format!("{index}/_refresh"))
            .await?;
        Ok(())
    }

    pub async fn refresh_table(&self, table_id: &DbTableId) -> Result<()> {
        let index = self.get_index_name(table_id);
        self.refresh_index(&index).await
    }

    /// This is not included in the trait because the
    /// script parameter is ES-specific.
    pub async fn partial_update_by_query(
        &self,
        table_id: &DbTableId,
        schema: &DbSchema,
        filter: &Filter,
        script: Value,
    ) -> Result<()> {
        let index = self.get_index_name(table_id);
        let esfilter = ElasticFilter::new(schema, filter)?;
        let _res: UpdateByQueryResponse = self
            .post(
                &format!("{index}/_update_by_query"),
                &json!({ "query": esfilter, "script": script }),
            )
            .await?;
        Ok(())
    }

    fn request(&self, method: Method, path: &str) -> RequestBuilder {
        let mut url = self.base_url.clone();
        url.set_path(path);
        let mut request = self.client.request(method, url);
        if let Some(usr) = &self.config.username {
            request = request.basic_auth(usr, self.config.password.as_ref());
        }
        request
    }

    async fn response<Res: DeserializeOwned>(&self, res: Response) -> Result<Res> {
        let is_success = res.status().is_success();
        let value = res.json::<Value>().await?;
        log::trace!("Response: {}", serde_json::to_string(&value).unwrap());
        match is_success {
            true => Ok(serde_json::from_value(value)?),
            false => Err(Error::EsError(serde_json::from_value(value)?)),
        }
    }

    async fn get<Res: DeserializeOwned>(&self, path: &str) -> Result<Res> {
        debug!("elasticsearch GET {path}");
        let res = self
            .request(Method::GET, path)
            .send()
            .map_err(Error::Request)
            .and_then(|res| self.response(res))
            .await;
        match &res {
            Ok(_) => debug!("elasticsearch GET {path} -> SUCCESS"),
            Err(e) => debug!("elasticsearch GET {path} -> FAILED: {e}"),
        }
        res
    }

    async fn head(&self, path: &str) -> Result<StatusCode> {
        debug!("elasticsearch HEAD {path}");
        let res = self
            .request(Method::HEAD, path)
            .send()
            .map_err(Error::Request)
            .await;
        match res {
            Ok(res) => {
                let status = res.status();
                match status.is_success() {
                    true => log::debug!("elasticsearch HEAD {path} -> SUCCESS"),
                    false => log::debug!("elasticsearch HEAD {path} -> FAILED: {status} status"),
                }
                Ok(status)
            }
            Err(e) => {
                debug!("elasticsearch HEAD {path} -> FAILED: {e}");
                Err(e)
            }
        }
    }

    async fn delete<Res: DeserializeOwned>(&self, path: &str) -> Result<Res> {
        debug!("elasticsearch DELETE {}", path);
        let res = self
            .request(Method::DELETE, path)
            .send()
            .map_err(Error::Request)
            .and_then(|res| self.response(res))
            .await;
        match &res {
            Ok(_) => debug!("elasticsearch DELETE {} -> SUCCESS", path),
            Err(e) => {
                debug!("elasticsearch DELETE {} -> FAILED: {}", path, e)
            }
        }
        res
    }

    async fn put<Req: Serialize, Res: DeserializeOwned>(
        &self,
        path: &str,
        req: &Req,
    ) -> Result<Res> {
        debug!("elasticsearch PUT {}", path);
        let res = self
            .request(Method::PUT, path)
            .json(req)
            .send()
            .map_err(Error::Request)
            .and_then(|res| self.response(res))
            .await;
        match &res {
            Ok(_) => debug!("elasticsearch PUT {} -> SUCCESS", path),
            Err(e) => {
                debug!("elasticsearch PUT {} -> FAILED: {}", path, e)
            }
        }
        res
    }

    async fn post<Req: Serialize, Res: DeserializeOwned>(
        &self,
        path: &str,
        req: &Req,
    ) -> Result<Res> {
        //debug!("elasticsearch POST {}", path);
        log::debug!(
            "elasticsearch POST {path}: {}",
            serde_json::to_string(&req).unwrap()
        );
        let res = self
            .request(Method::POST, path)
            .json(req)
            .send()
            .map_err(Error::Request)
            .and_then(|res| self.response(res))
            .await;
        match &res {
            Ok(_) => debug!("elasticsearch POST {path} -> SUCCESS"),
            Err(e) => {
                debug!("elasticsearch POST {path} -> FAILED: {e}")
            }
        }
        res
    }

    async fn post_with_query<Query: Serialize, Req: Serialize, Res: DeserializeOwned>(
        &self,
        path: &str,
        query: &Query,
        req: &Req,
    ) -> Result<Res> {
        let res = self
            .request(Method::POST, path)
            .query(query)
            .json(req)
            .send()
            .await?;
        self.response(res).await
    }

    async fn post_without_body<Query: Serialize, Res: DeserializeOwned>(
        &self,
        path: &str,
        query: &Query,
    ) -> Result<Res> {
        let res = self.request(Method::POST, path).query(query).send().await?;
        self.response(res).await
    }

    async fn post_ndjson<Req: Into<Body>, Res: DeserializeOwned>(
        &self,
        path: &str,
        req: Req,
    ) -> Result<Res> {
        debug!("elasticsearch POST {}", path);
        let res = self
            .request(Method::POST, path)
            .header("Content-Type", "application/x-ndjson")
            .body(req)
            .send()
            .map_err(Error::Request)
            .and_then(|res| self.response(res))
            .await;
        match &res {
            Ok(_) => debug!("elasticsearch POST {} -> SUCCESS", path),
            Err(e) => {
                debug!("elasticsearch POST {} -> FAILED: {}", path, e)
            }
        }
        res
    }

    async fn reindex(
        &self,
        old: &DbTableId,
        new: &DbTableId,
        old_schema: &DbSchema,
        new_schema: &DbSchema,
    ) -> Result<()> {
        let filter = Filter::All(Vec::new());
        let sort = json!([
            {"@current.from": "asc"},
            {"@active.from": "asc"},
            {"object_id.keyword": "asc"}
        ]);
        let (mut docs, mut query_state) = self
            .query_objects_first::<Value>(
                old,
                old_schema,
                &filter,
                &sort,
                Duration::from_secs(300),
                None,
            )
            .await?;

        loop {
            self.bulk_update(new, new_schema, docs).await?;
            match query_state {
                Some(state) => {
                    (docs, query_state) = self.query_objects_next::<Value>(state).await?;
                }
                None => break,
            }
        }

        Ok(())
    }

    pub fn whoami(&self) -> String {
        String::from("Elastic")
    }
}

impl DatabaseTrait for Database {
    type Error = Error;
    type Id = ElasticId;

    /* Setup and configuration. */

    async fn wait_for_database(&self) -> Result<()> {
        info!("Waiting for elastic database...");
        loop {
            tokio::select! {
                r = self.verify_database() => match r {
                    Ok(()) => {
                        info!("Elastic database is up and running!");
                        return Ok(())
                    },
                    Err(e) => {
                        info!("Error contacting elasticsearch: {}", e);
                        tokio::time::sleep(
                            std::time::Duration::from_secs(5)).await
                    },
                },
                _ = tokio::time::sleep(
                    std::time::Duration::from_secs(300)) => {
                    info!("Timeout (5min) waiting for elasticsearch :-(");
                    return Err(Error::Timeout);
                }
            }
        }
    }

    async fn verify_database(&self) -> Result<()> {
        let res: ClusterInfoResponse = self.get("/").await?;
        self.opensearch.store(
            matches!(res.version.distribution, ClusterDistribution::OpenSearch),
            std::sync::atomic::Ordering::Release,
        );
        Ok(())
    }

    /* Schema manipulation. */

    //https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html
    async fn create_table(&self, id: &DbTableId, definition: &DbTable) -> Result<()> {
        let index = self.get_index_name(id);
        info!("creating table: {index}");
        // Note: this is only true if the table does not yet exist!
        let req = CreateIndex {
            mappings: Some(ElasticMapping::new(&definition.schema())?),
            settings: Some(IndexSettings {
                total_fields_limit: Some(10000),
                ..IndexSettings::default()
            }),
            ..CreateIndex::default()
        };
        let _res: IndexResponse = self.put(&index, &req).await?;
        // assert!(res.acknowledged && res.index == table_id);
        Ok(())
    }

    async fn update_table(&self, id: &DbTableId, definition: &DbTable) -> Result<()> {
        let index = self.get_index_name(id);
        info!("updating mapping for table '{index}'");
        let _res: HashMap<String, ElasticMapping> = self
            .put(
                &format!("{index}/_mapping"),
                &ElasticMapping::new(&definition.schema())?,
            )
            .await?;
        Ok(())
    }

    async fn reindex_table(
        &self,
        id: &DbTableId,
        old_definition: &DbTable,
        new_definition: &DbTable,
    ) -> Result<()> {
        let reindexed = DbTableId::from_string(format!("{id}-reindex"));
        info!("reindexing table '{id}' to '{reindexed}'");

        let old_schema = old_definition.schema();
        let new_schema = new_definition.schema();

        // let create_req = CreateIndex {
        //     mappings: Some(ElasticMapping::new(&new_schema)?),
        //     settings: Some(IndexSettings {
        //         total_fields_limit: Some(10000),
        //         ..IndexSettings::default()
        //     }),
        //     ..CreateIndex::default()
        // };

        /* Reindex from table to table-reindex. */

        //let _res: IndexResponse = self.put(&reindexed, &create_req).await?;
        self.create_table(&reindexed, new_definition).await?;
        self.reindex(id, &reindexed, &old_schema, &new_schema)
            .await?;

        /* Reindex from table-reindex to table. */

        // let _res: Value = self.delete(id.to_str()).await?;
        // let _res: IndexResponse = self.put(id.to_str(), &create_req).await?;
        self.remove_table(id).await?;
        self.create_table(id, new_definition).await?;
        self.reindex(&reindexed, id, &new_schema, &new_schema)
            .await?;
        self.remove_table(&reindexed).await?;
        // let _res: Value = self.delete(&reindexed).await?;

        Ok(())
    }

    async fn remove_table(&self, id: &DbTableId) -> Result<()> {
        let index = self.get_index_name(id);
        info!("deleting index '{index}'");
        let _res: Value = self.delete(&index).await?;
        Ok(())
    }

    // https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-exists.html
    async fn has_table(&self, id: &DbTableId) -> Result<bool> {
        let index = self.get_index_name(id);
        info!("checking if table '{index}' exists");
        match self.head(&index).await {
            Ok(s) => Ok(s.as_u16() == 200),
            Err(e) => Err(e),
        }
    }

    /* Data manipulation. */

    // async fn insert_object<T: Serialize + Send + Sync>(
    //     &self,
    //     table_id: &DbTableId,
    //     schema: &DbSchema,
    //     doc_id: &Self::Id,
    //     value: T,
    // ) -> Result<()> {
    //     self.update_object(table_id, schema, doc_id, 0, value).await
    // }

    async fn update_object<T: Serialize + Send + Sync>(
        &self,
        table_id: &DbTableId,
        schema: &DbSchema,
        doc_id: &Self::Id,
        version: u64,
        value: T,
    ) -> Result<()> {
        let index = self.get_index_name(table_id);
        let value = ElasticValue::save(schema, serde_json::to_value(value)?)?;
        match self
            .post_with_query::<_, _, DocumentResponse>(
                &format!("{index}/_doc/{doc_id}"),
                &json!({
                    "version_type": "external",
                    "version": version
                }),
                &value,
            )
            .await
        {
            Ok(_res) => Ok(()),
            /* type == version_conflict_engine_exception */
            Err(Error::EsError(e)) if e.status == 409 => Ok(()),
            Err(e) => Err(e),
        }
    }

    async fn bulk_update<T, I>(
        &self,
        table_id: &DbTableId,
        schema: &DbSchema,
        updates: I,
    ) -> Result<()>
    where
        T: Serialize + Send + Sync,
        I: IntoIterator<Item = (Self::Id, u64, T)> + Send + Sync,
    {
        let index = self.get_index_name(table_id);
        let mut req = Vec::new();
        updates
            .into_iter()
            .try_for_each::<_, Result<()>>(|(id, version, value)| {
                let dbvalue = ElasticValue::save(schema, serde_json::to_value(&value)?)?;
                BulkOp::Index {
                    index: None,
                    id: id.0.as_str(),
                    value: &dbvalue,
                    version,
                }
                .write(&mut req)?;
                Ok(())
            })?;
        if !req.is_empty() {
            let res: BulkReponse = self.post_ndjson(&format!("{index}/_bulk"), req).await?;
            if res.errors
                && res
                    .items
                    .iter()
                    .any(|h| h.status() != 200 && h.status() != 409)
            {
                return Err(
                    match res
                        .items
                        .iter()
                        .all(|h| h.status() != 200 && h.status() != 409)
                    {
                        true => Error::BulkUpdateComplete,
                        false => Error::BulkUpdatePartial,
                    },
                );
            }
        }
        Ok(())
    }

    // async fn query_object<T: DeserializeOwned + Send + Sync>(
    //     &self,
    //     table_id: &DbTableId,
    //     schema: &DbSchema,
    //     filter: &Filter,
    //     sort: &Value,
    // ) -> Result<(Self::Id, u64, T)> {
    //     let matches = self
    //         .query_objects(table_id, schema, filter, sort, Some(2))
    //         .await?;

    //     match matches.len() {
    //         1 => {
    //             let (id, (version, value)) =
    //                 matches.into_iter().next().unwrap();
    //             Ok((id, version, value))
    //         }
    //         0 => Err(Error::ZeroHits),
    //         _ => Err(Error::ManyHits),
    //     }
    // }

    async fn query_objects<T: DeserializeOwned + Send + Sync>(
        &self,
        table_id: &DbTableId,
        schema: &DbSchema,
        filter: &Filter,
        sort: &Value,
        limit: Option<usize>,
    ) -> Result<Vec<(Self::Id, u64, T)>> {
        let index = self.get_index_name(table_id);
        match limit {
            Some(limit) if limit <= 10000 => {
                let esfilter = ElasticFilter::new(schema, filter)?;
                /* Todo: if filter is inexact (cannot be represented exactly
                 * in elastic query language), the limit is not correct! */
                let res: QueryResponse = self
                    .post_with_query(
                        &format!("{index}/_search"),
                        &json!({ "version": true }),
                        &json!({ "query": esfilter, "size": limit }),
                    )
                    .await?;

                /* Filter hits because the esfilter can be broader than the
                 * filter. Possible improvement: make ElasticFilter return
                 * whether it is inexact and only refilter if this is the case.
                 * Also, for inexact filters we should really use a scrolling
                 * query. */
                res.hits
                    .hits
                    .into_iter()
                    .filter_map(|hit| match hit.source.load(schema) {
                        Ok(val) => match filter.matches(schema, &val) {
                            Ok(true) => Some(Ok((hit.id, (hit.version, val)))),
                            Ok(false) => None,
                            Err(e) => Some(Err(e.into())),
                        },
                        Err(e) => Some(Err(e.into())),
                    })
                    .map(|res| {
                        res.and_then(|(id, (version, val))| {
                            Ok((id, version, serde_json::from_value(val)?))
                        })
                    })
                    .collect()
            }
            _ => {
                let (mut result, mut pit) = self
                    .query_objects_first(
                        table_id,
                        schema,
                        filter,
                        sort,
                        Duration::from_secs(60),
                        None,
                    )
                    .await?;
                while let Some(state) = pit.take() {
                    let (rows, next) = self.query_objects_next(state).await?;
                    result.extend(rows);
                    pit = next
                }
                Ok(result)
            }
        }
    }

    type QueryState<'a> = QueryState<'a>;

    async fn query_objects_first<'a, T: DeserializeOwned + Send + Sync>(
        &self,
        table_id: &DbTableId,
        schema: &'a DbSchema,
        filter: &'a Filter,
        sort: &'a Value,
        keep_alive: Duration,
        limit: Option<usize>,
    ) -> Result<(Vec<(Self::Id, u64, T)>, Option<Self::QueryState<'a>>)> {
        let index = self.get_index_name(table_id);
        let esfilter = ElasticFilter::new(schema, filter)?;
        let res: PitResponse = self
            .post_without_body(
                &match self.opensearch.load(std::sync::atomic::Ordering::Acquire) {
                    true => format!("{index}/_search/point_in_time"),
                    false => format!("{index}/_pit"),
                },
                &json!({ "keep_alive": format!("{}s", keep_alive.as_secs()) }),
            )
            .await?;
        let query_state = QueryState {
            pit_id: res.id,
            schema,
            filter,
            sort,
            limit,
            keep_alive,
            esfilter,
            last: None,
        };
        self.query_objects_next(query_state).await
    }

    async fn query_objects_next<'a, T: DeserializeOwned + Send + Sync>(
        &self,
        query_state: Self::QueryState<'a>,
    ) -> Result<(Vec<(Self::Id, u64, T)>, Option<Self::QueryState<'a>>)> {
        let res: QueryResponse = self
            .post_with_query(
                "_search",
                &json!({ "version": true }),
                &SearchRequest {
                    query: query_state.esfilter.clone(),
                    pit: Some(Pit {
                        id: query_state.pit_id.clone(),
                        keep_alive: format!("{}s", query_state.keep_alive.as_secs()),
                    }),
                    sort: Some(query_state.sort.clone()),
                    search_after: query_state.last.clone(),
                    size: Some(query_state.limit.map_or(10000, |n| n.min(10000))),
                },
            )
            .await?;
        let mut last = None;
        let docs: Vec<(ElasticId, u64, T)> = res
            .hits
            .hits
            .into_iter()
            .map(|hit| {
                last = Some(hit.sort.ok_or(Error::MissingSortField)?);
                let val = hit.source.load(query_state.schema)?;
                Ok(
                    match &query_state.filter.matches(query_state.schema, &val)? {
                        true => Some((hit.id, hit.version, serde_json::from_value(val)?)),
                        false => None,
                    },
                )
            })
            .filter_map(Result::transpose)
            .collect::<Result<_>>()?;
        let query_state = match docs.is_empty() {
            true => None,
            false => Some(QueryState {
                pit_id: res.pit_id.ok_or(Error::MissingPitId)?,
                limit: query_state.limit.map(|n| 0.max(n - docs.len())),
                last,
                ..query_state
            }),
        };
        Ok((docs, query_state))
    }
}

/// Elasticsearch options.
#[derive(Serialize, Deserialize, Args, Debug, Clone)]
pub struct DatabaseConfig {
    #[clap(env = "DB_ELASTIC_URL", long = "elastic-url")]
    pub url: String,
    #[clap(env = "DB_ELASTIC_USERNAME", long = "elastic-username")]
    pub username: Option<String>,
    #[clap(env = "DB_ELASTIC_PASSWORD", long = "elastic-password")]
    pub password: Option<String>,
    #[clap(env = "DB_ELASTIC_CERT", name = "elastic-cert", long = "elastic-cert")]
    pub cert: Option<String>,
    #[clap(env = "DB_ELASTIC_KEY", name = "elastic-key", long = "elastic-key")]
    pub key: Option<String>,
    #[clap(env = "DB_ELASTIC_CA", name = "elastic-ca", long = "elastic-ca")]
    pub ca: Option<String>,
    /// Prefix to use for all elasticsearch indices, to allow running
    /// multiple instances on the same elasticsearch cluster.
    #[clap(env = "DB_ELASTIC_INDEX_PREFIX", long = "elastic-index-prefix")]
    pub index_prefix: String,
}

#[derive(Debug, Clone)]
pub struct QueryState<'a> {
    pit_id: String,
    schema: &'a DbSchema,
    filter: &'a Filter,
    sort: &'a Value,
    esfilter: ElasticFilter,
    keep_alive: Duration,
    limit: Option<usize>,
    last: Option<Value>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "lowercase")]
pub enum WebProtocol {
    Http,
    Https,
}

impl fmt::Display for WebProtocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Http => write!(f, "http"),
            Self::Https => write!(f, "https"),
        }
    }
}

impl fmt::Display for ElasticId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
