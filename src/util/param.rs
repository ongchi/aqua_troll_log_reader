use num_derive::FromPrimitive;
use strum_macros::Display;

// Paramaters
// 1 Temperature
// 2 Pressure
// 3 Depth
// 4 Level, Depth to Water
// 5 Level, Surface Elevation
// 9 Actual Conductivity
// 10 Specific Conductivity
// 11 Resistivity
// 12 Salinity
// 13 Total Dissolved Solids
// 14 Density of Water
// 16 Barometric Pressure
// 17 pH
// 18 pH mV
// 19 Oxidation Reduction Potential
// 20 Dissolved Oxygen Concentration
// 21 Dissolved Oxygen % Saturation
// 24 Chloride (Cl-)
// 25 Turbidity
// 30 Oxygen Partial Pressure
// 31 Total Suspended Solids
// 32 External Voltage
// 33 Battery Capacity (remaining)
// 34 Rhodamine WT Concentration
// 35 Rhodamine WT Fluorescence Intensity
// 36 Chloride (Cl-) mV
// 37 Nitrate as Nitrogen (NO3--N) concentration
// 38 Nitrate (NO3-) mV
// 39 Ammonium as Nitrogen (NH4+-N) concentration
// 40 Ammonium (NH4) mV
// 41 Ammonia as Nitrogen (NH3-N) concentration
// 42 Total Ammonia as Nitrogen (NH3-N) concentration
// 48 Eh
// 49 Velocity
// 50 Chlorophyll-a Concentration
// 51 Chlorophyll-a Fluorescence Intensity
// 54 Blue Green Algae - Phycocyanin Concentration
// 55 Blue Green Algae - Phycocyanin Fluorescence Intensity
// 58 Blue Green Algae - Phycoerythrin Concentration
// 59 Blue Green Algae - Phycoerythrin Fluorescence Intensity
// 67 Fluorescein WT Concentration
// 68 Fluorescein WT Fluorescence Intensity
// 69 Fluorescent Dissolved Organic Matter Concentration
// 70 Fluorescent Dissolved Organic Matter Fluorescence Intensity
// 80 Crude Oil Concentration
// 81 Crude Oil Fluorescence Intensity
// 87 Colored Dissolved Organic Matter Concentration
#[repr(u8)]
#[derive(FromPrimitive, Display, Debug)]
pub enum Parameter {
    Temperature = 1,
    Pressure = 2,
    Depth = 3,
    #[strum(to_string = "Depth to Water")]
    DepthToWater = 4,
    #[strum(to_string = "Surface Elevation")]
    SurfaceElevation = 5,
    #[strum(to_string = "Actual Conductivity")]
    ActualConductivity = 9,
    #[strum(to_string = "Specific Conductivity")]
    SpecificConductivity = 10,
    Resistivity = 11,
    Salinity = 12,
    #[strum(to_string = "TDS")]
    TotalDissolvedSolids = 13,
    #[strum(to_string = "Density of Water")]
    DensityOfWater = 14,
    #[strum(to_string = "Barometric Pressure")]
    BarometricPressure = 16,
    #[strum(to_string = "pH")]
    PH = 17,
    #[strum(to_string = "pH(mV)")]
    PHmV = 18,
    #[strum(to_string = "ORP")]
    OxidationReductionPotential = 19,
    #[strum(to_string = "DO")]
    DissolvedOxygenConcentration = 20,
    #[strum(to_string = "DO % Saturation")]
    DissolvedOxygenPercentSaturation = 21,
    #[strum(to_string = "Cl⁻")]
    Chloride = 24,
    Turbidity = 25,
    #[strum(to_string = "pO₂")]
    OxygenPartialPressure = 30,
    #[strum(to_string = "TSS")]
    TotalSuspendedSolids = 31,
    #[strum(to_string = "External Voltage")]
    ExternalVoltage = 32,
    #[strum(to_string = "Battery Capacity")]
    BatteryCapacityRemaining = 33,
    #[strum(to_string = "Rhodamine WT")]
    RhodamineWTConcentration = 34,
    #[strum(to_string = "Rhodamine WT Fluorescence Intensity")]
    RhodamineWTFluorescenceIntensity = 35,
    #[strum(to_string = "Cl⁻ mV")]
    ChlorideMV = 36,
    #[strum(to_string = "NO₃⁻-N")]
    NitrateAsNitrogenConcentration = 37,
    #[strum(to_string = "NO₃⁻ mV")]
    NitrateMV = 38,
    #[strum(to_string = "NH₄⁺-N")]
    AmmoniumAsNitrogenConcentration = 39,
    #[strum(to_string = "NH₄ mV")]
    AmmoniumMV = 40,
    #[strum(to_string = "NH₃-N")]
    AmmoniaAsNitrogenConcentration = 41,
    #[strum(to_string = "Total NH₃-N")]
    TotalAmmoniaAsNitrogenConcentration = 42,
    Eh = 48,
    Velocity = 49,
    #[strum(to_string = "Chlorophyll-a")]
    ChlorophyllAConcentration = 50,
    #[strum(to_string = "Chlorophyll-a Fluorescence Intensity")]
    ChlorophyllAFluorescenceIntensity = 51,
    #[strum(to_string = "PC")]
    BlueGreenAlgaePhycocyaninConcentration = 54,
    #[strum(to_string = "PC Fluorescence Intensity")]
    BlueGreenAlgaePhycocyaninFluorescenceIntensity = 55,
    #[strum(to_string = "PE")]
    BlueGreenAlgaePhycoerythrinConcentration = 58,
    #[strum(to_string = "PE Fluorescence Intensity")]
    BlueGreenAlgaePhycoerythrinFluorescenceIntensity = 59,
    #[strum(to_string = "Fluorescein WT")]
    FluoresceinWTConcentration = 67,
    #[strum(to_string = "Fluorescein WT Fluorescence Intensity")]
    FluoresceinWTFluorescenceIntensity = 68,
    #[strum(to_string = "FDOM")]
    FluorescentDissolvedOrganicMatterConcentration = 69,
    #[strum(to_string = "FDOM Fluorescence Intensity")]
    FluorescentDissolvedOrganicMatterFluorescenceIntensity = 70,
    #[strum(to_string = "Crude Oil")]
    CrudeOilConcentration = 80,
    #[strum(to_string = "Crude Oil Fluorescence Intensity")]
    CrudeOilFluorescenceIntensity = 81,
    #[strum(to_string = "CDOM")]
    ColoredDissolvedOrganicMatterConcentration = 87,
}
