pub mod kingdom;
use uuid;

fn main() {
    let uuid = uuid::Uuid::new_v4();
    println!("{}", uuid);
    println!("Hello, world!");
}
