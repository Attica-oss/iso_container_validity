mod app;

fn main() {
    let validator = app::ContainerValidator::new();
    let container_numbers = validator.read_input(Some("transfer.parquet"));
    let results = validator.validate_container_numbers(container_numbers);

    for (number, is_valid) in results {
        println!("Container : {} , Valid: {}", number, is_valid);
    }
}
