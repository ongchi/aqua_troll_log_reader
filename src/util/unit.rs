use num_derive::FromPrimitive;
use strum_macros::Display;

// # Temperature
// 1 C Celsius
// 2 F Fahrenheit
// 3 K Kelvin
//
// # Pressure, Barometric Pressure
// 17 PSI Pounds per square inch
// 18 Pa Pascals
// 19 kPa Kilopascals
// 20 Bar Bars
// 21 mBar Millibars
// 22 mmHg Millimeters of Mercury (0 to C)
// 23 inHg Inches of Mercury (4 to C)
// 24 cmH2O Centimeters of water (4 to C)
// 25 inH2O Inches of water (4 to C)
// 26 Torr Torr
// 27 atm Standard atmosphere
//
// # Distance/Length
// 33 mm Millimeters
// 34 cm Centimeters
// 35 m Meters
// 36 km Kilometer
// 37 in Inches
// 38 ft Feet
//
// # Coordinates
// 49 deg Degrees
// 50 min Minutes
// 51 sec Seconds
//
// # Conductivity
// 65 µS/cm Microsiemens per centimeter
// 66 mS/cm Millisiemens per centimeter
//
// # Resistivity
// 81 ohm-cm Ohm-centimeters
//
// # Salinity
// 97 PSU Practical Salinity Units
// 98 ppt Parts per thousand salinity
//
// # Concentration
// 113 ppm Parts per million
// 114 ppt Parts per thousand
// 115 (Available)
// 116 (Available)
// 117 mg/L Milligrams per liter
// 118 µg/L Micrograms per liter
// 119 --- (Deprecated)
// 120 g/L Grams per liter
// 121 ppb Parts per billion
//
// # Density
// 129 g/cm3 Grams per cubic centimeter
//
// # pH
// 145 pH pH
//
// # Voltage
// 161 µV Microvolts
// 162 mV Millivolts
// 163 V Volts
//
// # Dissolved Oxygen (DO) % Saturation
// 177 % sat Percent saturation
//
// # Turbidity
// 193 FNU Formazin nephelometric units
// 194 NTU Nephelometric turbidity units
// 195 FTU Formazin turbidity units
//
// # Flow
// 209 ft3/s Cubic feet per second
// 210 (Available - was Cubic feet per minute)
// 211 (Available - was Cubic feet per hour)
// 212 ft3/day Cubic feet per day
// 213 gal/s Gallons per second
// 214 gal/min Gallons per minute
// 215 gal/hr Gallons per hour
// 216 MGD Millions of gallons per day
// 217 m3/sec Cubic meters per second
// 218 (Available - was Cubic meters per minute)
// 219 m3/hr Cubic meters per hour
// 220 (Available - was Cubic meters per day)
// 221 L/s Liters per second
// 222 ML/day Millions of liters per day
// 223 mL/min Milliliters per minute
// 224 kL/day Thousands of liters per day
//
// # Volume
// 225 ft3 Cubic feet
// 226 gal Gallons
// 227 Mgal Millions of gallons
// 228 m3 Cubic meters
// 229 L Liters
// 230 acre-ft Acre feet
// 231 mL Milliliters
// 232 ML Millions of liters
// 233 kL Thousands of liters
// 234 Acre-in Acre inches
//
// # Percentage
// 241 % Percent
//
// # Fluorescence
// 257 RFU Relative Fluorescence Units
//
// # Low-Flow
// 273 mL/sec Milliliters per second
// 274 mL/hr Milliliters per hour
// 275 L/min Liters per minute
// 276 L/hr Liters per hour
//
// # Current
// 289 µA Microamps
// 290 mA Milliamps
// 291 A Amps
//
// # Velocity
// 305 ft/s Feet per second
// 306 m/s Meters per second
#[derive(FromPrimitive, Display, Debug)]
#[repr(u16)]
pub enum Unit {
    #[strum(to_string = "°C")]
    Celsius = 1,
    #[strum(to_string = "°F")]
    Fahrenheit = 2,
    #[strum(to_string = "°K")]
    Kelvin = 3,
    #[strum(to_string = "psi")]
    PoundsPerSquareInch = 17,
    #[strum(to_string = "Pa")]
    Pascals = 18,
    #[strum(to_string = "kPa")]
    Kilopascals = 19,
    #[strum(to_string = "Bar")]
    Bars = 20,
    #[strum(to_string = "mBar")]
    Millibars = 21,
    #[strum(to_string = "mmHg")]
    MillimetersOfMercury = 22,
    #[strum(to_string = "inHg")]
    InchesOfMercury = 23,
    #[strum(to_string = "cmH₂O")]
    CentimetersOfWater = 24,
    #[strum(to_string = "inH₂O")]
    InchesOfWater = 25,
    Torr = 26,
    #[strum(to_string = "atm")]
    StandardAtmosphere = 27,
    #[strum(to_string = "mm")]
    Millimeters = 33,
    #[strum(to_string = "cm")]
    Centimeters = 34,
    #[strum(to_string = "m")]
    Meters = 35,
    #[strum(to_string = "km")]
    Kilometer = 36,
    #[strum(to_string = "in")]
    Inches = 37,
    #[strum(to_string = "ft")]
    Feet = 38,
    #[strum(to_string = "deg")]
    Degrees = 49,
    #[strum(to_string = "min")]
    Minutes = 50,
    #[strum(to_string = "sec")]
    Seconds = 51,
    #[strum(to_string = "µS/cm")]
    MicrosiemensPerCentimeter = 65,
    #[strum(to_string = "mS/cm")]
    MillisiemensPerCentimeter = 66,
    #[strum(to_string = "Ω-cm")]
    OhmCentimeters = 81,
    #[strum(to_string = "PSU")]
    PracticalSalinityUnits = 97,
    #[strum(to_string = "ppt sal")]
    PartsPerThousandSalinity = 98,
    #[strum(to_string = "ppm")]
    PartsPerMillion = 113,
    #[strum(to_string = "ppt")]
    PartsPerThousand = 114,
    #[strum(to_string = "mg/L")]
    MilligramsPerLiter = 117,
    #[strum(to_string = "µg/L")]
    MicrogramsPerLiter = 118,
    #[strum(to_string = "g/L")]
    GramsPerLiter = 120,
    #[strum(to_string = "ppb")]
    PartsPerBillion = 121,
    #[strum(to_string = "g/cm³")]
    GramsPerCubicCentimeter = 129,
    #[strum(to_string = "pH")]
    PH = 145,
    #[strum(to_string = "µV")]
    Microvolts = 161,
    #[strum(to_string = "mV")]
    Millivolts = 162,
    #[strum(to_string = "V")]
    Volts = 163,
    #[strum(to_string = "DO % sat")]
    DissolvedOxygenPercentSaturation = 177,
    #[strum(to_string = "FNU")]
    FormazinNephelometricUnits = 193,
    #[strum(to_string = "NTU")]
    NephelometricTurbidityUnits = 194,
    #[strum(to_string = "FTU")]
    FormazinTurbidityUnits = 195,
    #[strum(to_string = "ft³/s")]
    CubicFeetPerSecond = 209,
    #[strum(to_string = "ft³/day")]
    CubicFeetPerDay = 212,
    #[strum(to_string = "gal/s")]
    GallonsPerSecond = 213,
    #[strum(to_string = "gal/min")]
    GallonsPerMinute = 214,
    #[strum(to_string = "gal/hr")]
    GallonsPerHour = 215,
    #[strum(to_string = "MGD")]
    MillionsOfGallonsPerDay = 216,
    #[strum(to_string = "m³/s")]
    CubicMetersPerSecond = 217,
    #[strum(to_string = "m³/hr")]
    CubicMetersPerHour = 219,
    #[strum(to_string = "L/s")]
    LitersPerSecond = 221,
    #[strum(to_string = "ML/day")]
    MillionsOfLitersPerDay = 222,
    #[strum(to_string = "mL/min")]
    MillilitersPerMinute = 223,
    #[strum(to_string = "kL/day")]
    ThousandsOfLitersPerDay = 224,
    #[strum(to_string = "ft³")]
    CubicFeet = 225,
    #[strum(to_string = "gal")]
    Gallons = 226,
    #[strum(to_string = "Mgal")]
    MillionsOfGallons = 227,
    #[strum(to_string = "m³")]
    CubicMeters = 228,
    #[strum(to_string = "L")]
    Liters = 229,
    #[strum(to_string = "acre-ft")]
    AcreFeet = 230,
    #[strum(to_string = "mL")]
    Milliliters = 231,
    #[strum(to_string = "ML")]
    MillionsOfLiters = 232,
    #[strum(to_string = "kL")]
    ThousandsOfLiters = 233,
    #[strum(to_string = "Acre-in")]
    AcreInches = 234,
    #[strum(to_string = "%")]
    Percent = 241,
    #[strum(to_string = "RFU")]
    RelativeFluorescenceUnits = 257,
    #[strum(to_string = "mL/sec")]
    MillilitersPerSecond = 273,
    #[strum(to_string = "mL/hr")]
    MillilitersPerHour = 274,
    #[strum(to_string = "L/min")]
    LitersPerMinute = 275,
    #[strum(to_string = "L/hr")]
    LitersPerHour = 276,
    #[strum(to_string = "µA")]
    Microamps = 289,
    #[strum(to_string = "mA")]
    Milliamps = 290,
    #[strum(to_string = "A")]
    Amps = 291,
    #[strum(to_string = "ft/s")]
    FeetPerSecond = 305,
    #[strum(to_string = "m/s")]
    MetersPerSecond = 306,
}
