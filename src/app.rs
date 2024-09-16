use polars::prelude::*;
use regex::Regex;
use serde::Deserialize;
use std::fs;
use std::io::{self, Write};

#[derive(Debug, Deserialize)]
struct SocConfig {
    soc: SocContainers,
}

#[derive(Debug, Deserialize)]
struct SocContainers {
    containers: Vec<String>,
}

pub struct ContainerValidator;

impl ContainerValidator {
    const SOC_FILE: &'static str = "app/SOC.toml";

    pub fn new() -> Self {
        ContainerValidator
    }

    pub fn read_input(&self, file_path: Option<&str>) -> Vec<String> {
        if let Some(path) = file_path {
            // Using polars to read the parquet file
            let df = LazyFrame::scan_parquet(path, Default::default())
                .expect("Error reading parquet file.")
                .collect()
                .expect("Failed to collec the Dataframe.");

            let series = df
                .column("container_number")
                .expect("Failed to get the column.");

            let unique_series = series.unique().expect("Failed to get the unique series.");

            return unique_series
                .expect("Error converting series to utf8.")
                .into_iter()
                .map(|s| s.unwrap().to_string())
                .collect();
        }

        println!("Enter container numbers separated by a comma: ");
        io::stdout().flush().unwrap();
        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();
        input
            .trim()
            .split(',')
            .map(|s| s.trim().to_string())
            .collect()
    }

    pub fn read_toml_file(&self) -> Vec<String> {
        let contents = fs::read_to_string(Self::SOC_FILE).expect("Error reading toml file.");
        let config: SocConfig = toml::from_str(&contents).expect("Error parsing toml file.");

        config.soc.containers
    }

    pub fn validate_container_number(&self, container_number: &str) -> bool {
        let pattern = Regex::new(r"^[0-9]{4}-[0-9]{4}-[0-9]$").unwrap();

        if container_number.starts_with("XXXX") {
            return self.validate_soc_number(container_number);
        }

        if container_number.len() != 11 {
            eprintln!(
                "Container number '{}' is not 11 characters long.",
                container_number
            );
            return false;
        }

        if !pattern.is_match(container_number) {
            eprintln!(
                "Container number '{}' is not a valid format.",
                container_number
            );
            return false;
        }

        if !self.validate_check_digit(container_number) {
            eprintln!(
                "Container number '{}' is not a valid check digit.",
                container_number
            );
            return false;
        }

        true
    }

    pub fn validate_soc_number(&self, container_number: &str) -> bool {
        let soc_pattern = Regex::new(r"^XXXX[0-9]+$").unwrap();

        if !soc_pattern.is_match(container_number) {
            eprintln!(
                "Container number '{}' is not a valid SOC number.",
                container_number
            );
            return false;
        }

        let valid_soc_numbers = self.read_toml_file();
        if !valid_soc_numbers.contains(&container_number.to_string()) {
            eprintln!(
                "Container number '{}' is not in the list of SOC numbers.",
                container_number
            );
            return false;
        }

        true
    }

    pub fn validate_check_digit(&self, container_number: &str) -> bool {
        let value_map: std::collections::HashMap<char, i32> = vec![
            ('A', 10),
            ('B', 12),
            ('C', 13),
            ('D', 14),
            ('E', 15),
            ('F', 16),
            ('G', 17),
            ('H', 18),
            ('I', 19),
            ('J', 20),
            ('K', 21),
            ('L', 23),
            ('M', 24),
            ('N', 25),
            ('O', 26),
            ('P', 27),
            ('Q', 28),
            ('R', 29),
            ('S', 30),
            ('T', 31),
            ('U', 32),
            ('V', 34),
            ('W', 35),
            ('X', 36),
            ('Y', 37),
            ('Z', 38),
        ]
        .into_iter()
        .collect();

        let mut total_sum = 0;
        for (i, ch) in container_number.chars().enumerate().take(10) {
            if i < 4 {
                total_sum += value_map.get(&ch).unwrap() * (2_i32.pow(i as u32));
            } else {
                total_sum += ch.to_digit(10).unwrap() as i32 * (2_i32.pow(i as u32));
            }
        }

        let check_digit = total_sum % 11 % 10;
        check_digit
            == container_number
                .chars()
                .nth(10)
                .unwrap()
                .to_digit(10)
                .unwrap() as i32
    }

    pub fn validate_container_numbers(
        &self,
        container_numbers: Vec<String>,
    ) -> std::collections::HashMap<String, bool> {
        let mut results = std::collections::HashMap::new();
        for number in container_numbers {
            let is_valid = self.validate_container_number(&number);
            results.insert(number, is_valid);
        }
        results
    }
}
