use crate::commons::flight;
use crate::servers::flightsql::helpers::{to_tonic_error, FetchResults};
use crate::state::DQStateRef;
use anyhow::Error;
use arrow::array::builder::StringBuilder;
use arrow::array::{ArrayRef, RecordBatch};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::sql::metadata::{
    SqlInfoData, SqlInfoDataBuilder, XdbcTypeInfo, XdbcTypeInfoData, XdbcTypeInfoDataBuilder,
};
use arrow_flight::sql::server::PeekableFlightDataStream;
use arrow_flight::sql::{
    server::FlightSqlService, ActionBeginSavepointRequest, ActionBeginSavepointResult,
    ActionBeginTransactionRequest, ActionBeginTransactionResult, ActionCancelQueryRequest,
    ActionCancelQueryResult, ActionClosePreparedStatementRequest,
    ActionCreatePreparedStatementRequest, ActionCreatePreparedStatementResult,
    ActionCreatePreparedSubstraitPlanRequest, ActionEndSavepointRequest,
    ActionEndTransactionRequest, Any, CommandGetCatalogs, CommandGetCrossReference,
    CommandGetDbSchemas, CommandGetExportedKeys, CommandGetImportedKeys, CommandGetPrimaryKeys,
    CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables, CommandGetXdbcTypeInfo,
    CommandPreparedStatementQuery, CommandPreparedStatementUpdate, CommandStatementQuery,
    CommandStatementSubstraitPlan, CommandStatementUpdate, Nullable, ProstMessageExt, Searchable,
    SqlInfo, TicketStatementQuery, XdbcDataType,
};
use arrow_flight::{
    flight_service_server::FlightService, Action, FlightData, FlightDescriptor, FlightEndpoint,
    FlightInfo, HandshakeRequest, HandshakeResponse, IpcMessage, Location, SchemaAsIpc, Ticket,
};
use arrow_ipc::writer::IpcWriteOptions;
use arrow_ipc::CompressionType;
use arrow_schema::{DataType, Field, Schema};
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use futures::{stream, Stream, TryStreamExt};
use once_cell::sync::Lazy;
use prost::Message;
use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use tonic::{Request, Response, Status, Streaming};

macro_rules! status {
    ($desc:expr, $err:expr) => {
        Status::internal(format!("{}: {} at {}:{}", $desc, $err, file!(), line!()))
    };
}

const SIMPLE_TOKEN: &str = "simple_token";
const SIMPLE_HANDLE: &str = "simple_handle";
const SIMPLE_UPDATE_RESULT: i64 = 1;

static SIMPLE_SQL_DATA: Lazy<SqlInfoData> = Lazy::new(|| {
    let mut builder = SqlInfoDataBuilder::new();
    builder.append(SqlInfo::FlightSqlServerName, "Simple Flight SQL Server");
    builder.append(SqlInfo::FlightSqlServerVersion, "1");
    builder.append(SqlInfo::FlightSqlServerArrowVersion, "1.3");
    builder.build().unwrap()
});

static SIMPLE_XBDC_DATA: Lazy<XdbcTypeInfoData> = Lazy::new(|| {
    let mut builder = XdbcTypeInfoDataBuilder::new();
    builder.append(XdbcTypeInfo {
        type_name: "INTEGER".into(),
        data_type: XdbcDataType::XdbcInteger,
        column_size: Some(32),
        literal_prefix: None,
        literal_suffix: None,
        create_params: None,
        nullable: Nullable::NullabilityNullable,
        case_sensitive: false,
        searchable: Searchable::Full,
        unsigned_attribute: Some(false),
        fixed_prec_scale: false,
        auto_increment: Some(false),
        local_type_name: Some("INTEGER".into()),
        minimum_scale: None,
        maximum_scale: None,
        sql_data_type: XdbcDataType::XdbcInteger,
        datetime_subcode: None,
        num_prec_radix: Some(2),
        interval_precision: None,
    });
    builder.build().unwrap()
});

static SIMPLE_TABLES: Lazy<Vec<&'static str>> = Lazy::new(|| vec!["deltaquery.simple.test0"]);

#[derive(Clone)]
pub struct FlightSqlServiceSimple {}

impl FlightSqlServiceSimple {
    pub async fn try_new(_state: DQStateRef, _catalog: serde_yaml::Value) -> Result<Self, Error> {
        Ok(FlightSqlServiceSimple {})
    }

    fn check_token<T>(&self, request: &Request<T>) -> Result<(), Error> {
        let basic = "Basic ";
        let authorization = request.metadata().get("authorization").unwrap().to_str()?;
        if !authorization.starts_with(basic) {}
        let payload = BASE64_STANDARD.decode(&authorization[basic.len()..])?;
        let payload = String::from_utf8(payload)?;
        let tokens: Vec<_> = payload.split(':').collect();
        #[allow(unused_variables)]
        let (username, password) = match tokens.as_slice() {
            [username, password] => (username, password),
            _ => (&"none", &"none"),
        };

        Ok(())
    }

    fn get_dummy_batch() -> Result<RecordBatch, Error> {
        let schema = Schema::new(vec![Field::new("salutation", DataType::Utf8, false)]);
        let mut builder = StringBuilder::new();
        builder.append_value("Hello, FlightSQL!");
        let cols = vec![Arc::new(builder.finish()) as ArrayRef];
        let batches = RecordBatch::try_new(Arc::new(schema), cols)?;
        Ok(batches)
    }
}

#[tonic::async_trait]
impl FlightSqlService for FlightSqlServiceSimple {
    type FlightService = FlightSqlServiceSimple;

    async fn do_handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        log::info!("do_handshake");

        let basic = "Basic ";
        let authorization = request
            .metadata()
            .get("authorization")
            .ok_or_else(|| Status::invalid_argument("Authorization field not present"))?
            .to_str()
            .map_err(|e| status!("Authorization not parsable", e))?;
        if !authorization.starts_with(basic) {
            Err(Status::invalid_argument(format!(
                "Auth type not implemented: {authorization}"
            )))?;
        }
        let payload = BASE64_STANDARD
            .decode(&authorization[basic.len()..])
            .map_err(|e| status!("Authorization not decodable", e))?;
        let payload =
            String::from_utf8(payload).map_err(|e| status!("Authorization not parsable", e))?;
        let tokens: Vec<_> = payload.split(':').collect();
        #[allow(unused_variables)]
        let (username, password) = match tokens.as_slice() {
            [username, password] => (username, password),
            _ => Err(Status::invalid_argument(
                "Invalid authorization header".to_string(),
            ))?,
        };

        let result = HandshakeResponse {
            protocol_version: 0,
            payload: SIMPLE_TOKEN.into(),
        };
        let result = Ok(result);

        let stream: Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>> =
            Box::pin(stream::iter(vec![result]));

        let res = Response::new(stream);
        log::info!("response={:#?}", res.metadata());
        Ok(res)
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        log::info!("get_flight_info_statement");
        log::info!("query={:#?}", query);
        log::info!("request={:#?}", request);

        let _ = self.check_token(&request).map_err(to_tonic_error)?;
        let batch = Self::get_dummy_batch().map_err(to_tonic_error)?;
        let schema = (*batch.schema()).clone();
        let num_rows = batch.num_rows();
        let num_bytes = batch.get_array_memory_size();
        let loc = Location {
            uri: "grpc://127.0.0.1:32010".to_string(),
        };
        let fetch = FetchResults {
            handle: SIMPLE_HANDLE.to_string(),
        };
        let ticket = Ticket {
            ticket: fetch.as_any().encode_to_vec().into(),
        };
        let endpoint = FlightEndpoint {
            ticket: Some(ticket),
            location: vec![loc],
        };
        let flight_info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| status!("Unable to serialize schema", e))?
            .with_descriptor(FlightDescriptor::new_cmd(vec![]))
            .with_endpoint(endpoint)
            .with_total_records(num_rows as i64)
            .with_total_bytes(num_bytes as i64)
            .with_ordered(false);

        let res = Response::new(flight_info);
        log::info!("response={:#?}", res);
        Ok(res)
    }

    async fn get_flight_info_substrait_plan(
        &self,
        query: CommandStatementSubstraitPlan,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        log::info!("get_flight_info_substrait_plan");
        log::info!("query={:#?}", query);
        log::info!("request={:#?}", request);

        Err(Status::unimplemented(
            "get_flight_info_substrait_plan not implemented",
        ))
    }

    async fn get_flight_info_prepared_statement(
        &self,
        cmd: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        log::info!("get_flight_info_prepared_statement");
        log::info!("cmd={:#?}", cmd);
        log::info!("request={:#?}", request);

        let _ = self.check_token(&request).map_err(to_tonic_error)?;
        let handle = std::str::from_utf8(&cmd.prepared_statement_handle)
            .map_err(|e| status!("Unable to parse handle", e))?;
        let batch = Self::get_dummy_batch().map_err(to_tonic_error)?;
        let schema = (*batch.schema()).clone();
        let num_rows = batch.num_rows();
        let num_bytes = batch.get_array_memory_size();
        let loc = Location {
            uri: "grpc://127.0.0.1".to_string(),
        };
        let fetch = FetchResults {
            handle: handle.to_string(),
        };
        let ticket = Ticket {
            ticket: fetch.as_any().encode_to_vec().into(),
        };
        let endpoint = FlightEndpoint {
            ticket: Some(ticket),
            location: vec![loc],
        };
        let flight_info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| status!("Unable to serialize schema", e))?
            .with_descriptor(FlightDescriptor::new_cmd(vec![]))
            .with_endpoint(endpoint)
            .with_total_records(num_rows as i64)
            .with_total_bytes(num_bytes as i64)
            .with_ordered(false);

        let res = Response::new(flight_info);
        log::info!("response={:#?}", res);
        Ok(res)
    }

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        log::info!("get_flight_info_catalogs");
        log::info!("query={:#?}", query);
        log::info!("request={:#?}", request);

        let flight_descriptor = request.into_inner();
        let ticket = Ticket {
            ticket: query.encode_to_vec().into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(&query.into_builder().schema())
            .map_err(|e| status!("Unable to encode schema", e))?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(Response::new(flight_info))
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        log::info!("get_flight_info_schemas");
        log::info!("query={:#?}", query);
        log::info!("request={:#?}", request);

        let flight_descriptor = request.into_inner();
        let ticket = Ticket {
            ticket: query.encode_to_vec().into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(&query.into_builder().schema())
            .map_err(|e| status!("Unable to encode schema", e))?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(Response::new(flight_info))
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        log::info!("get_flight_info_tables");
        log::info!("query={:#?}", query);
        log::info!("request={:#?}", request);

        let flight_descriptor = request.into_inner();
        let ticket = Ticket {
            ticket: query.encode_to_vec().into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(&query.into_builder().schema())
            .map_err(|e| status!("Unable to encode schema", e))?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(Response::new(flight_info))
    }

    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        log::info!("get_flight_info_table_types");
        log::info!("query={:#?}", query);
        log::info!("request={:#?}", request);

        Err(Status::unimplemented(
            "get_flight_info_table_types not implemented",
        ))
    }

    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        log::info!("get_flight_info_sql_info");
        log::info!("query={:#?}", query);
        log::info!("request={:#?}", request);

        let flight_descriptor = request.into_inner();
        let ticket = Ticket::new(query.as_any().encode_to_vec());
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(query.into_builder(&SIMPLE_SQL_DATA).schema().as_ref())
            .map_err(|e| status!("Unable to encode schema", e))?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        let res = Response::new(flight_info);
        log::info!("response={:#?}", res);
        Ok(res)
    }

    async fn get_flight_info_primary_keys(
        &self,
        query: CommandGetPrimaryKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        log::info!("get_flight_info_primary_keys");
        log::info!("query={:#?}", query);
        log::info!("request={:#?}", request);

        Err(Status::unimplemented(
            "get_flight_info_primary_keys not implemented",
        ))
    }

    async fn get_flight_info_exported_keys(
        &self,
        query: CommandGetExportedKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        log::info!("get_flight_info_exported_keys");
        log::info!("query={:#?}", query);
        log::info!("request={:#?}", request);

        Err(Status::unimplemented(
            "get_flight_info_exported_keys not implemented",
        ))
    }

    async fn get_flight_info_imported_keys(
        &self,
        query: CommandGetImportedKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        log::info!("get_flight_info_imported_keys");
        log::info!("query={:#?}", query);
        log::info!("request={:#?}", request);

        Err(Status::unimplemented(
            "get_flight_info_imported_keys not implemented",
        ))
    }

    async fn get_flight_info_cross_reference(
        &self,
        query: CommandGetCrossReference,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        log::info!("get_flight_info_cross_reference");
        log::info!("query={:#?}", query);
        log::info!("request={:#?}", request);

        Err(Status::unimplemented(
            "get_flight_info_imported_keys not implemented",
        ))
    }

    async fn get_flight_info_xdbc_type_info(
        &self,
        query: CommandGetXdbcTypeInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        log::info!("get_flight_info_xdbc_type_info");
        log::info!("query={:#?}", query);
        log::info!("request={:#?}", request);

        let flight_descriptor = request.into_inner();
        let ticket = Ticket::new(query.encode_to_vec());
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(query.into_builder(&SIMPLE_XBDC_DATA).schema().as_ref())
            .map_err(|e| status!("Unable to encode schema", e))?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(Response::new(flight_info))
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        log::info!("do_get_statement");
        log::info!("ticket={:#?}", ticket);
        log::info!("request={:#?}", request);

        Err(Status::unimplemented("do_get_statement not implemented"))
    }

    async fn do_get_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        log::info!("do_get_prepared_statement");
        log::info!("query={:#?}", query);
        log::info!("request={:#?}", request);

        Err(Status::unimplemented(
            "do_get_prepared_statement not implemented",
        ))
    }

    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        log::info!("do_get_catalogs");
        log::info!("query={:#?}", query);
        log::info!("request={:#?}", request);

        let catalog_names = SIMPLE_TABLES
            .iter()
            .map(|full_name| full_name.split('.').collect::<Vec<_>>()[0].to_string())
            .collect::<HashSet<_>>();
        let mut builder = query.into_builder();
        for catalog_name in catalog_names {
            builder.append(catalog_name);
        }
        let schema = builder.schema();
        let batch = builder.build();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async { batch }))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        log::info!("do_get_schemas");
        log::info!("query={:#?}", query);
        log::info!("request={:#?}", request);

        let schemas = SIMPLE_TABLES
            .iter()
            .map(|full_name| {
                let parts = full_name.split('.').collect::<Vec<_>>();
                (parts[0].to_string(), parts[1].to_string())
            })
            .collect::<HashSet<_>>();

        let mut builder = query.into_builder();
        for (catalog_name, schema_name) in schemas {
            builder.append(catalog_name, schema_name);
        }

        let schema = builder.schema();
        let batch = builder.build();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async { batch }))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        log::info!("do_get_tables");
        log::info!("query={:#?}", query);
        log::info!("request={:#?}", request);

        let tables = SIMPLE_TABLES
            .iter()
            .map(|full_name| {
                let parts = full_name.split('.').collect::<Vec<_>>();
                (
                    parts[0].to_string(),
                    parts[1].to_string(),
                    parts[2].to_string(),
                )
            })
            .collect::<HashSet<_>>();

        let dummy_schema = Schema::empty();
        let mut builder = query.into_builder();
        for (catalog_name, schema_name, table_name) in tables {
            builder
                .append(
                    catalog_name,
                    schema_name,
                    table_name,
                    "TABLE",
                    &dummy_schema,
                )
                .map_err(Status::from)?;
        }

        let schema = builder.schema();
        let batch = builder.build();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async { batch }))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_get_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        log::info!("do_get_table_types");
        log::info!("query={:#?}", query);
        log::info!("request={:#?}", request);

        Err(Status::unimplemented("do_get_table_types not implemented"))
    }

    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        log::info!("do_get_sql_info");
        log::info!("query={:#?}", query);
        log::info!("request={:#?}", request);

        let builder = query.into_builder(&SIMPLE_SQL_DATA);
        let schema = builder.schema();
        let batch = builder.build();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async { batch }))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_get_primary_keys(
        &self,
        query: CommandGetPrimaryKeys,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        log::info!("do_get_primary_keys");
        log::info!("query={:#?}", query);
        log::info!("request={:#?}", request);

        Err(Status::unimplemented("do_get_primary_keys not implemented"))
    }

    async fn do_get_exported_keys(
        &self,
        query: CommandGetExportedKeys,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        log::info!("do_get_exported_keys");
        log::info!("query={:#?}", query);
        log::info!("request={:#?}", request);

        Err(Status::unimplemented(
            "do_get_exported_keys not implemented",
        ))
    }

    async fn do_get_imported_keys(
        &self,
        query: CommandGetImportedKeys,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        log::info!("do_get_imported_keys");
        log::info!("query={:#?}", query);
        log::info!("request={:#?}", request);

        Err(Status::unimplemented(
            "do_get_imported_keys not implemented",
        ))
    }

    async fn do_get_cross_reference(
        &self,
        query: CommandGetCrossReference,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        log::info!("do_get_cross_reference");
        log::info!("query={:#?}", query);
        log::info!("request={:#?}", request);

        Err(Status::unimplemented(
            "do_get_cross_reference not implemented",
        ))
    }

    async fn do_get_xdbc_type_info(
        &self,
        query: CommandGetXdbcTypeInfo,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        log::info!("do_get_xdbc_type_info");
        log::info!("query={:#?}", query);
        log::info!("request={:#?}", request);

        let builder = query.into_builder(&SIMPLE_XBDC_DATA);
        let schema = builder.schema();
        let batch = builder.build();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async { batch }))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_get_fallback(
        &self,
        request: Request<Ticket>,
        message: Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        log::info!("do_get_fallback");
        log::info!("request={:#?}", request);
        log::info!("message={:#?}", message);

        let _ = self.check_token(&request).map_err(to_tonic_error)?;
        let batch = Self::get_dummy_batch().map_err(to_tonic_error)?;
        let schema = batch.schema();
        let batches = vec![batch];
        let batches = Arc::new(batches);
        let flight_data =
            flight::batches_to_flight_data(schema.as_ref(), batches, Some(CompressionType::ZSTD))
                .map_err(|e| status!("Could not convert batches", e))?
                .into_iter()
                .map(Ok);

        log::info!("data={:#?}", flight_data);

        let stream: Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>> =
            Box::pin(stream::iter(flight_data));

        let res = Response::new(stream);
        Ok(res)
    }

    async fn do_put_statement_update(
        &self,
        ticket: CommandStatementUpdate,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        log::info!("do_put_statement_update");
        log::info!("ticket={:#?}", ticket);

        Ok(SIMPLE_UPDATE_RESULT)
    }

    async fn do_put_substrait_plan(
        &self,
        ticket: CommandStatementSubstraitPlan,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        log::info!("do_put_substrait_plan");
        log::info!("ticket={:#?}", ticket);

        Err(Status::unimplemented(
            "do_put_substrait_plan not implemented",
        ))
    }

    async fn do_put_prepared_statement_query(
        &self,
        query: CommandPreparedStatementQuery,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<Response<<Self as FlightService>::DoPutStream>, Status> {
        log::info!("do_put_prepared_statement_query");
        log::info!("query={:#?}", query);

        Err(Status::unimplemented(
            "do_put_prepared_statement_query not implemented",
        ))
    }

    async fn do_put_prepared_statement_update(
        &self,
        query: CommandPreparedStatementUpdate,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        log::info!("do_put_prepared_statement_update");
        log::info!("query={:#?}", query);

        Err(Status::unimplemented(
            "do_put_prepared_statement_update not implemented",
        ))
    }

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        log::info!("do_action_create_prepared_statement");
        log::info!("query={:#?}", query);
        log::info!("request={:#?}", request);

        let _ = self.check_token(&request).map_err(to_tonic_error)?;
        let schema = Self::get_dummy_batch().map_err(to_tonic_error)?.schema();
        let message = SchemaAsIpc::new(&schema, &IpcWriteOptions::default())
            .try_into()
            .map_err(|e| status!("Unable to serialize schema", e))?;
        let IpcMessage(schema_bytes) = message;
        let res = ActionCreatePreparedStatementResult {
            prepared_statement_handle: SIMPLE_HANDLE.into(),
            dataset_schema: schema_bytes,
            parameter_schema: Default::default(),
        };

        log::info!("response={:#?}", res);

        Ok(res)
    }

    async fn do_action_close_prepared_statement(
        &self,
        query: ActionClosePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<(), Status> {
        log::info!("do_action_close_prepared_statement");
        log::info!("query={:#?}", query);
        log::info!("request={:#?}", request);

        Err(Status::unimplemented(
            "do_action_close_prepared_statement not implemented",
        ))
    }

    async fn do_action_create_prepared_substrait_plan(
        &self,
        query: ActionCreatePreparedSubstraitPlanRequest,
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        log::info!("do_action_create_prepared_substrait_plan");
        log::info!("query={:#?}", query);
        log::info!("request={:#?}", request);

        Err(Status::unimplemented(
            "do_action_create_prepared_substrait_plan not implemented",
        ))
    }

    async fn do_action_begin_transaction(
        &self,
        query: ActionBeginTransactionRequest,
        request: Request<Action>,
    ) -> Result<ActionBeginTransactionResult, Status> {
        log::info!("do_action_begin_transaction");
        log::info!("query={:#?}", query);
        log::info!("request={:#?}", request);

        Err(Status::unimplemented(
            "do_action_begin_transaction not implemented",
        ))
    }

    async fn do_action_end_transaction(
        &self,
        query: ActionEndTransactionRequest,
        request: Request<Action>,
    ) -> Result<(), Status> {
        log::info!("do_action_end_transaction");
        log::info!("query={:#?}", query);
        log::info!("request={:#?}", request);

        Err(Status::unimplemented(
            "do_action_end_transaction not implemented",
        ))
    }

    async fn do_action_begin_savepoint(
        &self,
        query: ActionBeginSavepointRequest,
        request: Request<Action>,
    ) -> Result<ActionBeginSavepointResult, Status> {
        log::info!("do_action_begin_savepoint");
        log::info!("query={:#?}", query);
        log::info!("request={:#?}", request);

        Err(Status::unimplemented(
            "do_action_begin_savepoint not implemented",
        ))
    }

    async fn do_action_end_savepoint(
        &self,
        query: ActionEndSavepointRequest,
        request: Request<Action>,
    ) -> Result<(), Status> {
        log::info!("do_action_end_savepoint");
        log::info!("query={:#?}", query);
        log::info!("request={:#?}", request);

        Err(Status::unimplemented(
            "do_action_end_savepoint not implemented",
        ))
    }

    async fn do_action_cancel_query(
        &self,
        query: ActionCancelQueryRequest,
        request: Request<Action>,
    ) -> Result<ActionCancelQueryResult, Status> {
        log::info!("do_action_cancel_query");
        log::info!("query={:#?}", query);
        log::info!("request={:#?}", request);

        Err(Status::unimplemented(
            "do_action_cancel_query not implemented",
        ))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {
        log::info!("register_sql_info");
    }
}
