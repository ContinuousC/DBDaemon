use std::{collections::HashMap, fs, iter::FromIterator};

use clap::Parser;
use dbdaemon_api::{js_backend_db_service_stub, py_backend_db_service_stub};
use rpc::Template;

/// Generate ContinuousC dbdaemon api definitions.
#[derive(Parser, Debug)]
struct Args {
    /// Path to a source template file.
    file: Vec<String>,
}

fn main() {
    let args = Args::parse();
    for path in &args.file {
        if let Some(base) = path.strip_suffix(".tmpl.js") {
            let data = fs::read_to_string(path).unwrap();
            let tmpl = Template::parse(&data);
            let src = tmpl.fill(HashMap::from_iter([(
                "DbService",
                js_backend_db_service_stub(),
            )]));
            fs::write(format!("{base}.js"), src).unwrap();
        } else if let Some(base) = path.strip_suffix(".tmpl.py") {
            let data = fs::read_to_string(path).unwrap();
            let tmpl = Template::parse(&data);
            let src = tmpl.fill(HashMap::from_iter([(
                "DbService",
                py_backend_db_service_stub(),
            )]));
            fs::write(format!("{base}.py"), src).unwrap();
        } else {
            eprintln!("Warning: ignoring file with unsupported suffix: {path}");
        }
    }
}
