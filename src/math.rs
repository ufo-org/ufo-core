use num::Integer;

// pub fn div_floor<T>(x: T, div: T) -> T
// where
//     T: Integer + Clone,
// {
//     x.prev_multiple_of(&div) / div
// }

pub fn div_ceil<T>(x: T, div: T) -> T
where
    T: Integer + Clone,
{
    x.next_multiple_of(&div) / div
}
