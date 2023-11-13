use crate::error::DQError;
use arrow_array::RecordBatch;
use arrow_flight::{FlightData, SchemaAsIpc};
use arrow_ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use arrow_ipc::{CompressionType, MetadataVersion};
use arrow_schema::Schema;
use arrow_select::concat::concat_batches;
use std::sync::Arc;

pub fn batches_to_flight_data(
    schema: &Schema,
    batches: Vec<RecordBatch>,
    compression: Option<CompressionType>,
) -> Result<Vec<FlightData>, DQError> {
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?
        .try_with_compression(compression)?;
    let schema_flight_data: FlightData = SchemaAsIpc::new(schema, &options).into();
    let mut dictionaries = vec![];
    let mut flight_data = vec![];

    let generator = IpcDataGenerator::default();
    let mut dictionary_tracker = DictionaryTracker::new(false);

    let mut encoded_size: usize = 0;
    let mut dictionaries_size: usize = 0;

    for batch in batches.iter() {
        let (encoded_dictionaries, encoded_batch) =
            generator.encoded_batch(batch, &mut dictionary_tracker, &options)?;

        encoded_size = encoded_size + encoded_batch.arrow_data.len();
        dictionaries_size = dictionaries_size
            + encoded_dictionaries
                .iter()
                .map(|d| d.arrow_data.len())
                .reduce(|a, b| a + b)
                .unwrap_or(0);

        dictionaries.extend(encoded_dictionaries.into_iter().map(Into::into));
        flight_data.push(encoded_batch.into());
    }

    log::debug!(
        "batches={}, encoded={}, dictionaries={}",
        batches.len(),
        encoded_size,
        dictionaries_size,
    );

    let mut stream = vec![schema_flight_data];
    stream.extend(dictionaries);
    stream.extend(flight_data);
    let flight_data: Vec<_> = stream.into_iter().collect();
    Ok(flight_data)
}
