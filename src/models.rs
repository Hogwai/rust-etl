use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct VehicleRecord {
    #[serde(rename = "VIN (1-10)")]
    pub vin: String,
    pub county: String,
    pub city: String,
    pub state: String,
    #[serde(rename = "Postal Code")]
    pub postal_code: String,
    #[serde(rename = "Model Year")]
    pub model_year: Option<u16>,
    pub make: String,
    pub model: String,
    #[serde(rename = "Electric Vehicle Type")]
    pub electric_vehicle_type: String,
    #[serde(rename = "Clean Alternative Fuel Vehicle (CAFV) Eligibility")]
    pub cafv_eligibility: String,
    #[serde(rename = "Electric Range")]
    pub electric_range: Option<u16>,
    #[serde(rename = "Base MSRP")]
    pub base_msrp: Option<u32>,
    #[serde(rename = "Legislative District")]
    pub legislative_district: Option<u16>,
    #[serde(rename = "DOL Vehicle ID")]
    pub dol_vehicle_id: Option<u64>,
    #[serde(rename = "Vehicle Location")]
    pub vehicle_location: String,
    #[serde(rename = "Electric Utility")]
    pub electric_utility: String,
    #[serde(rename = "2020 Census Tract")]
    pub census_tract: String,
}

impl VehicleRecord {
    pub fn is_eligible(&self, min_range: u16) -> bool {
        self.electric_range.unwrap_or(0) >= min_range
    }

    pub fn has_valid_data(&self) -> bool {
        self.electric_range.is_some()
            && self.model_year.is_some()
            && self.dol_vehicle_id.is_some()
    }
}