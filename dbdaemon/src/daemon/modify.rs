/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use std::{
    mem::{self, MaybeUninit},
    panic::{catch_unwind, AssertUnwindSafe},
};

/// Move an initialized value out of a mutable reference, leaving uninit
/// in its place.
unsafe fn uninit_move<T>(value: &mut T) -> (T, &mut MaybeUninit<T>) {
    let ptr: &mut MaybeUninit<T> = mem::transmute(value);
    let val = mem::replace(ptr, MaybeUninit::uninit()).assume_init();
    (val, ptr)
}

/// Modify a value in a mutable reference using a function taking an
/// owned value. This is used to avoid a clone on the original
/// value. Panic safety is ensured by aborting the program if the
/// closure panics (like `take_mut::take`).
pub fn modify<F, T>(value: &mut T, f: F)
where
    F: FnOnce(T) -> T,
{
    let (value, ptr) = unsafe { uninit_move(value) };
    let value =
        catch_unwind(AssertUnwindSafe(|| f(value))).unwrap_or_else(|_| std::process::abort());
    ptr.write(value);
}

/// Modify a value in a mutable reference, like `modify`, but return a
/// result as well. This is used to avoid a clone on the original
/// value. Panic safety is ensured by aborting the program if the
/// closure panics (like `take_mut::take`).
pub fn modify_res<F, T, R>(value: &mut T, f: F) -> R
where
    F: FnOnce(T) -> (T, R),
{
    let (value, ptr) = unsafe { uninit_move(value) };
    let (v, r) =
        catch_unwind(AssertUnwindSafe(|| f(value))).unwrap_or_else(|_| std::process::abort());
    ptr.write(v);
    r
}
