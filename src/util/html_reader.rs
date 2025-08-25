use std::io::{Read, Seek};

use arrow::array::RecordBatch;
use num_traits::FromPrimitive;
use scraper::{CaseSensitivity, Html, Selector};
use serde_json::{Map, Value};

use super::param::Parameter;
use super::unit::Unit;
use crate::{error::AquaTrollLogError, util::common::TableBuilder};

// Log reader for In-Situ HTML files
// ref: https://in-situ.com/en/html-parsing-guide
pub(crate) fn read_html<R: Read>(
    reader: &mut R,
) -> Result<(Map<String, Value>, RecordBatch), AquaTrollLogError> {
    let mut buf = vec![];
    let _ = reader.read_to_end(&mut buf)?;

    let mut attr_headers: Vec<String> = vec![];
    let mut attrs: Vec<Map<String, Value>> = vec![];

    // convert bytes into string
    let html = String::from_utf8(buf)?;
    let document = Html::parse_document(&html);
    let header_selector = Selector::parse("tr").unwrap();
    let data_selector = Selector::parse("td").unwrap();

    let mut table_builder = TableBuilder::new();

    for row in document.select(&header_selector) {
        let is_section_header = row
            .value()
            .has_class("sectionHeader", CaseSensitivity::AsciiCaseInsensitive);
        let is_section_member = row
            .value()
            .has_class("sectionMember", CaseSensitivity::AsciiCaseInsensitive);
        let is_data_header = row
            .value()
            .has_class("dataHeader", CaseSensitivity::AsciiCaseInsensitive);
        let is_data = row
            .value()
            .has_class("data", CaseSensitivity::AsciiCaseInsensitive);

        if is_section_header {
            let header = row.text().collect::<String>();
            attr_headers.push(header);
            attrs.push(Map::new());
        } else if is_section_member {
            // TODO: Parse values by HTML property
            let cur_attr = attrs
                .last_mut()
                .ok_or(AquaTrollLogError::SectionHeaderNotFound)?;
            row.text()
                .collect::<String>()
                .split_once("=")
                .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
                .map(|(k, v)| cur_attr.insert(k, Value::String(v)))
                .ok_or(AquaTrollLogError::InvalidData)?;
        } else if is_data_header || is_data {
            let data = row
                .select(&data_selector)
                .map(|h| h.text().collect::<String>())
                .collect();

            if is_data_header {
                let attrs: Vec<&str> = row
                    .select(&data_selector)
                    .map(|h| h.attr("isi-data-column-header").unwrap_or(""))
                    .collect();

                let params: Vec<Option<Parameter>> = row
                    .select(&data_selector)
                    .map(|h| h.attr("isi-parameter-type").unwrap_or(""))
                    .map(|v| v.parse().unwrap_or(0))
                    .map(Parameter::from_u8)
                    .collect();

                let units: Vec<Option<Unit>> = row
                    .select(&data_selector)
                    .map(|h| h.attr("isi-unit-type").unwrap_or(""))
                    .map(|v| v.parse().unwrap_or(0))
                    .map(Unit::from_u16)
                    .collect();

                let serials: Vec<Option<u64>> = row
                    .select(&data_selector)
                    .map(|h| h.attr("isi-sensor-serial-number").unwrap_or(""))
                    .map(|v| v.parse().ok())
                    .collect();

                let mut fields: Vec<String> = vec![];
                for (a, (p, (u, s))) in attrs
                    .into_iter()
                    .zip(params.into_iter().zip(units.into_iter().zip(serials)))
                {
                    let field_name = if let Some(param) = p {
                        if let Some(unit) = u {
                            if let Some(serial) = s {
                                format!("{} ({}) ({})", param, unit, serial)
                            } else {
                                format!("{} ({})", param, unit)
                            }
                        } else {
                            param.to_string()
                        }
                    } else if a == "DateTime" {
                        "Date Time".to_string()
                    } else if a == "Marked" {
                        "Marked".to_string()
                    } else {
                        let n_unknown = fields.iter().filter(|s| s.starts_with("Unknown")).count();
                        if n_unknown > 0 {
                            "Unknown_{:02}".to_string()
                        } else {
                            "Unknown".to_string()
                        }
                    };
                    fields.push(field_name);
                }

                // TODO: Extract sensor serial number from field name
                table_builder = table_builder.field_names(fields);
            } else {
                table_builder = table_builder.try_push_row(data)?;
            }
        }
    }

    let log_data = table_builder.try_build()?;

    let mut attr = Map::new();
    for (k, v) in attr_headers.into_iter().zip(attrs) {
        attr.insert(k, Value::Object(v));
    }

    Ok((attr, log_data))
}

pub(crate) fn read_zipped_html<R: Read + Seek>(
    reader: R,
) -> Result<(Map<String, Value>, RecordBatch), AquaTrollLogError> {
    let mut zip = zip::ZipArchive::new(reader)?;
    let mut html_file = zip.by_index(0)?;

    read_html(&mut html_file)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use serde_json::json;

    use super::*;

    const TEST_CONTENT: &str = r#"
<html>
    <head></head>
    <body>
        <table>
        <tr class="sectionHeader"><td isi-group="LocationProperties">Location Properties</td></tr>
        <tr class="sectionMember"><td isi-group-member="LocationProperties" isi-property="Name" isi-text-node=""><span isi-label="">Location Name</span> = <span isi-value="">Device Location</span></td></tr>
        <tr class="sectionHeader"><td isi-group="ReportProperties">Report Properties</td></tr>
        <tr class="sectionMember"><td isi-group-member="ReportProperties" isi-property="StartTime" isi-timestamp="113276523905024"><span isi-label="">Start Time</span> = <span isi-value="">2024-10-09 16:29:44</span></td></tr>
        <tr class="sectionMember"><td isi-group-member="ReportProperties" isi-property="TimeOffset" isi-timespan-milliseconds="28800000"><span isi-label="">Time Offset</span> = <span isi-value="">08:00:00</span></td></tr>
        <tr class="sectionMember"><td isi-group-member="ReportProperties" isi-property="Duration" isi-timespan-milliseconds="2106000"><span isi-label="">Duration</span> = <span isi-value="">00:35:06</span></td></tr>
        <tr class="sectionMember"><td isi-group-member="ReportProperties" isi-property="Readings" isi-text-node=""><span isi-label="">Readings</span> = <span isi-value="">1053</span></td></tr>
        <tr class="dataHeader" isi-data-table="">
            <td isi-data-column-header="DateTime">Date Time</td>
            <td isi-data-column-header="Parameter" isi-device-serial-number="999996" isi-sensor-serial-number="999997" isi-sensor-type="56" isi-parameter-type="9" isi-unit-type="65">Actual Conductivity (µS/cm) (999997)</td>
            <td isi-data-column-header="Parameter" isi-device-serial-number="999996" isi-sensor-serial-number="999997" isi-sensor-type="56" isi-parameter-type="10" isi-unit-type="65">Specific Conductivity (µS/cm) (999997)</td>
            <td isi-data-column-header="Parameter" isi-device-serial-number="999996" isi-sensor-serial-number="999997" isi-sensor-type="56" isi-parameter-type="12" isi-unit-type="97">Salinity (PSU) (999997)</td>
            <td isi-data-column-header="Parameter" isi-device-serial-number="999996" isi-sensor-serial-number="999997" isi-sensor-type="56" isi-parameter-type="11" isi-unit-type="81">Resistivity (Ω⋅cm) (999997)</td>
            <td isi-data-column-header="Parameter" isi-device-serial-number="999996" isi-sensor-serial-number="999997" isi-sensor-type="56" isi-parameter-type="14" isi-unit-type="129">Density (g/cm³) (999997)</td>
            <td isi-data-column-header="Parameter" isi-device-serial-number="999996" isi-sensor-serial-number="999997" isi-sensor-type="56" isi-parameter-type="13" isi-unit-type="113">Total Dissolved Solids (ppm) (999997)</td>
            <td isi-data-column-header="Parameter" isi-device-serial-number="999996" isi-sensor-serial-number="999995" isi-sensor-type="57" isi-parameter-type="20" isi-unit-type="117">RDO Concentration (mg/L) (999995)</td>
            <td isi-data-column-header="Parameter" isi-device-serial-number="999996" isi-sensor-serial-number="999995" isi-sensor-type="57" isi-parameter-type="21" isi-unit-type="177">RDO Saturation (%Sat) (999995)</td>
            <td isi-data-column-header="Parameter" isi-device-serial-number="999996" isi-sensor-serial-number="999995" isi-sensor-type="57" isi-parameter-type="30" isi-unit-type="26">Oxygen Partial Pressure (Torr) (999995)</td>
            <td isi-data-column-header="Parameter" isi-device-serial-number="999996" isi-sensor-serial-number="999991" isi-sensor-type="58" isi-parameter-type="17" isi-unit-type="145">pH (pH) (999991)</td>
            <td isi-data-column-header="Parameter" isi-device-serial-number="999996" isi-sensor-serial-number="999991" isi-sensor-type="58" isi-parameter-type="18" isi-unit-type="162">pH mV (mV) (999991)</td>
            <td isi-data-column-header="Parameter" isi-device-serial-number="999996" isi-sensor-serial-number="999991" isi-sensor-type="58" isi-parameter-type="19" isi-unit-type="162">ORP (mV) (999991)</td>
            <td isi-data-column-header="Parameter" isi-device-serial-number="999996" isi-sensor-serial-number="999998" isi-sensor-type="50" isi-parameter-type="25" isi-unit-type="194">Turbidity (NTU) (999998)</td>
            <td isi-data-column-header="Parameter" isi-device-serial-number="999996" isi-sensor-serial-number="999996" isi-sensor-type="79" isi-parameter-type="1" isi-unit-type="1">Temperature (°C) (999996)</td>
            <td isi-data-column-header="Parameter" isi-device-serial-number="999996" isi-sensor-serial-number="999996" isi-sensor-type="59" isi-parameter-type="16" isi-unit-type="22">Barometric Pressure (mm Hg) (999996)</td>
            <td isi-data-column-header="Parameter" isi-device-serial-number="999996" isi-sensor-serial-number="999999" isi-sensor-type="54" isi-parameter-type="2" isi-unit-type="17">Pressure (psi) (999999)</td>
            <td isi-data-column-header="Parameter" isi-device-serial-number="999996" isi-sensor-serial-number="999999" isi-sensor-type="54" isi-parameter-type="3" isi-unit-type="35">Depth (m) (999999)</td>
            <td isi-data-column-header="Parameter" isi-device-serial-number="999996" isi-sensor-serial-number="999996" isi-sensor-type="79" isi-parameter-type="32" isi-unit-type="163">External Voltage (V) (999996)</td>
            <td isi-data-column-header="Parameter" isi-device-serial-number="999996" isi-sensor-serial-number="999996" isi-sensor-type="79" isi-parameter-type="33" isi-unit-type="241">Battery Capacity (%) (999996)</td>
            <td isi-data-column-header="Marked">Marked</td>
        </tr>
        <tr class="data" isi-data-row="" isi-timestamp="113276524036096"><td class="dateTime">2024-10-09 16:29:46</td><td isi-data-quality="4">0</td><td isi-data-quality="4">0</td><td isi-data-quality="4">4.656613E-10</td><td isi-data-quality="4">10000000</td><td isi-data-quality="4">0.9970099</td><td isi-data-quality="4">0</td><td isi-data-quality="4">8.945552</td><td isi-data-quality="4">106.4632</td><td isi-data-quality="4">143.49069</td><td>6.4217362</td><td>29.850006</td><td isi-data-quality="5">123.78549</td><td isi-data-quality="4">4.6494746</td><td>25.14812</td><td>774.4906</td><td>14.673946</td><td>10.333733</td><td>0.189</td><td>88</td><td></td></tr>
        <tr class="data" isi-data-row="" isi-timestamp="113276524167168"><td class="dateTime">2024-10-09 16:29:48</td><td isi-data-quality="4">0</td><td isi-data-quality="4">0</td><td isi-data-quality="4">4.656613E-10</td><td isi-data-quality="4">10000000</td><td isi-data-quality="4">0.9970099</td><td isi-data-quality="4">0</td><td isi-data-quality="4">8.945552</td><td isi-data-quality="4">106.4632</td><td isi-data-quality="4">143.49069</td><td>6.4217362</td><td>29.850006</td><td isi-data-quality="5">123.78549</td><td isi-data-quality="4">4.6494746</td><td>25.14812</td><td>774.4906</td><td>14.673946</td><td>10.333733</td><td>0.189</td><td>88</td><td></td></tr>
        </table>
    </body>
</html>
    "#;

    #[test]
    fn log_html() {
        let mut reader = Cursor::new(TEST_CONTENT.as_bytes());
        let (attr, log_data) = read_html(&mut reader).unwrap();

        // Check attributes of log file
        assert_eq!(
            serde_json::to_string(&attr).unwrap(),
            serde_json::to_string(&json!({
                "Location Properties": {
                    "Location Name": "Device Location",
                },
                "Report Properties": {
                    "Start Time": "2024-10-09 16:29:44",
                    "Time Offset": "08:00:00",
                    "Duration": "00:35:06",
                    "Readings": "1053"
                },
            }))
            .unwrap()
        );

        // Check schema of table data
        assert_eq!(
            log_data
                .schema()
                .fields
                .into_iter()
                .map(|f| f.name().to_string())
                .collect::<Vec<String>>(),
            vec![
                "Date Time",
                "Actual Conductivity (µS/cm) (999997)",
                "Specific Conductivity (µS/cm) (999997)",
                "Salinity (PSU) (999997)",
                "Resistivity (Ω-cm) (999997)",
                "Density of Water (g/cm³) (999997)",
                "TDS (ppm) (999997)",
                "DO (mg/L) (999995)",
                "DO % Saturation (DO % sat) (999995)",
                "pO₂ (Torr) (999995)",
                "pH (pH) (999991)",
                "pH(mV) (mV) (999991)",
                "ORP (mV) (999991)",
                "Turbidity (NTU) (999998)",
                "Temperature (°C) (999996)",
                "Barometric Pressure (mmHg) (999996)",
                "Pressure (psi) (999999)",
                "Depth (m) (999999)",
                "External Voltage (V) (999996)",
                "Battery Capacity (%) (999996)",
                "Marked"
            ]
        );
    }
}
