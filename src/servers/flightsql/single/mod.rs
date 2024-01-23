use crate::commons::flight;
use crate::commons::tonic::to_tonic_error;
use crate::servers::flightsql::helpers::FetchResults;
use crate::state::DQState;
use anyhow::Error;
use arrow::array::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::sql::metadata::{SqlInfoData, SqlInfoDataBuilder};
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
    CommandStatementSubstraitPlan, CommandStatementUpdate, ProstMessageExt, SqlInfo,
    TicketStatementQuery,
};
use arrow_flight::{
    flight_service_server::FlightService, Action, FlightData, FlightDescriptor, FlightEndpoint,
    FlightInfo, HandshakeRequest, HandshakeResponse, Location, Ticket,
};
use arrow_ipc::CompressionType;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use futures::{stream, Stream, TryStreamExt};
use once_cell::sync::Lazy;
use prost::Message;
use serde::{Deserialize, Serialize};
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status, Streaming};
use uuid::Uuid;

macro_rules! status {
    ($desc:expr, $err:expr) => {
        Status::internal(format!("{}: {} at {}:{}", $desc, $err, file!(), line!()))
    };
}

static BASIC_AUTHORIZATION_PREFIX: &str = "Basic ";

static SQL_INFO_DATA: Lazy<SqlInfoData> = Lazy::new(|| {
    let mut builder = SqlInfoDataBuilder::new();
    builder.append(SqlInfo::FlightSqlServerName, "Single Flight SQL Server");
    builder.append(SqlInfo::FlightSqlServerVersion, "1");
    builder.append(SqlInfo::FlightSqlServerArrowVersion, "1.3");
    builder.build().unwrap()
});

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FlightSqlServiceSingleConfig {
    compression: Option<String>,
    endpoint: Option<String>,
    port: Option<u16>,
}

#[derive(Clone)]
pub struct FlightSqlServiceSingle {
    state: Arc<Mutex<DQState>>,

    compression: Option<CompressionType>,
    endpoint: String,

    handles: Arc<Mutex<HashMap<String, Vec<RecordBatch>>>>,
}

impl FlightSqlServiceSingle {
    pub async fn new(state: Arc<Mutex<DQState>>, catalog: serde_yaml::Value) -> Self {
        let config: FlightSqlServiceSingleConfig = serde_yaml::from_value(catalog).unwrap();

        let compression = match config.compression.as_deref() {
            Some("zstd") => Some(CompressionType::ZSTD),
            Some(&_) => None,
            None => None,
        };
        let endpoint = match config.endpoint {
            Some(endpoint) => endpoint.clone(),
            None => local_ip_address::local_ip()
                .expect("could not fetch local ip address")
                .to_string(),
        };
        let port = match config.port {
            Some(port) => port,
            None => 32010,
        };

        FlightSqlServiceSingle {
            state,
            compression,
            endpoint: format!("grpc://{}:{}", endpoint, port),
            handles: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn build_flight_info(
        &self,
        items: &Vec<RecordBatch>,
        handle: String,
        location: String,
    ) -> Result<Option<FlightInfo>, Error> {
        if let Some(item0) = items.first() {
            let schema = (*item0.schema()).clone();
            let num_rows: usize = items.iter().map(|b| b.num_rows()).sum();
            let num_bytes: usize = items.iter().map(|b| b.get_array_memory_size()).sum();

            let location = Location { uri: location };
            let fetch = FetchResults { handle: handle };
            let ticket = Ticket::new(fetch.as_any().encode_to_vec());
            let endpoint = FlightEndpoint {
                ticket: Some(ticket),
                location: vec![location],
            };
            let flight_info = FlightInfo::new()
                .try_with_schema(&schema)?
                .with_descriptor(FlightDescriptor::new_cmd(vec![]))
                .with_endpoint(endpoint)
                .with_total_records(num_rows as i64)
                .with_total_bytes(num_bytes as i64)
                .with_ordered(false);

            Ok(Some(flight_info))
        } else {
            Ok(None)
        }
    }

    fn check_token<T>(&self, request: &Request<T>) -> Result<(), Error> {
        if let Some(authorization) = request.metadata().get("authorization") {
            let authorization = authorization.to_str()?;
            if authorization.starts_with(BASIC_AUTHORIZATION_PREFIX) {
                let payload =
                    BASE64_STANDARD.decode(&authorization[BASIC_AUTHORIZATION_PREFIX.len()..])?;
                let payload = String::from_utf8(payload)?;
                let tokens: Vec<_> = payload.split(':').collect();
                #[allow(unused_variables)]
                let (username, password) = match tokens.as_slice() {
                    [username, password] => (username, password),
                    _ => (&"none", &"none"),
                };
            }
        }

        Ok(())
    }

    fn parse_sql(&self, sql: &String) -> Result<Vec<Statement>, Error> {
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql)?;

        Ok(statements)
    }
}

#[tonic::async_trait]
impl FlightSqlService for FlightSqlServiceSingle {
    type FlightService = FlightSqlServiceSingle;

    async fn do_handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        log::info!("do_handshake");
        log::info!("request={:#?}", request);

        let authorization = request
            .metadata()
            .get("authorization")
            .ok_or_else(|| Status::invalid_argument("Authorization field not present"))?
            .to_str()
            .map_err(|e| status!("Authorization not parsable", e))?;
        if !authorization.starts_with(BASIC_AUTHORIZATION_PREFIX) {
            Err(Status::invalid_argument(format!(
                "Auth type not implemented: {authorization}"
            )))?;
        }
        let payload = BASE64_STANDARD
            .decode(&authorization[BASIC_AUTHORIZATION_PREFIX.len()..])
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
            payload: "".into(),
        };
        let result = Ok(result);

        let stream: Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>> =
            Box::pin(stream::iter(vec![result]));

        let mut res = Response::new(stream);
        res.metadata_mut()
            .insert("authorization", authorization.parse().unwrap());
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

        let statements = self.parse_sql(&query.query).map_err(to_tonic_error)?;
        for statement in statements.iter() {
            log::info!("statement={:#?}", statement.to_string());

            match statement {
                Statement::Query(_) => {
                    let handle: String = Uuid::new_v4().to_string();

                    let mut compute = self
                        .state
                        .lock()
                        .await
                        .get_compute()
                        .await
                        .expect("could not get compute engine");

                    let batches = compute
                        .execute(statement, self.state.clone())
                        .await
                        .map_err(to_tonic_error)?;

                    if let Ok(Some(flight_info)) =
                        self.build_flight_info(&batches, handle.clone(), self.endpoint.clone())
                    {
                        let mut handles = self.handles.lock().await;
                        handles.insert(handle.clone(), batches);

                        let res = Response::new(flight_info);
                        log::info!("response={:#?}", res);
                        return Ok(res);
                    }
                }
                _ => unimplemented!(),
            }
        }

        Err(Status::internal("invalid sql"))
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

        Err(Status::unimplemented(
            "get_flight_info_prepared_statement not implemented",
        ))
    }

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        log::info!("get_flight_info_catalogs");
        log::info!("query={:#?}", query);
        log::info!("request={:#?}", request);

        Err(Status::unimplemented(
            "get_flight_info_catalogs not implemented",
        ))
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        log::info!("get_flight_info_schemas");
        log::info!("query={:#?}", query);
        log::info!("request={:#?}", request);

        Err(Status::unimplemented(
            "get_flight_info_schemas not implemented",
        ))
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        log::info!("get_flight_info_tables");
        log::info!("query={:#?}", query);
        log::info!("request={:#?}", request);

        Err(Status::unimplemented(
            "get_flight_info_tables not implemented",
        ))
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
            .try_with_schema(query.into_builder(&SQL_INFO_DATA).schema().as_ref())
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

        Err(Status::unimplemented(
            "get_flight_info_xdbc_type_info not implemented",
        ))
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

        Err(Status::unimplemented("do_get_catalogs not implemented"))
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        log::info!("do_get_schemas");
        log::info!("query={:#?}", query);
        log::info!("request={:#?}", request);

        Err(Status::unimplemented("do_get_schemas not implemented"))
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        log::info!("do_get_tables");
        log::info!("query={:#?}", query);
        log::info!("request={:#?}", request);

        Err(Status::unimplemented("do_get_tables not implemented"))
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

        let builder = query.into_builder(&SQL_INFO_DATA);
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

        Err(Status::unimplemented(
            "do_get_xdbc_type_info not implemented",
        ))
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

        if let Some(fetch_results) = message.unpack::<FetchResults>().unwrap() {
            let mut handles = self.handles.lock().await;

            if let Some(batches) = handles.remove(&fetch_results.handle) {
                if let Some(batch0) = batches.first() {
                    let schema = batch0.schema();
                    let num_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                    let num_bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
                    log::info!("schema={:#?}", schema);
                    let flight_data =
                        flight::batches_to_flight_data(schema.as_ref(), batches, self.compression)
                            .map_err(|e| status!("Could not convert batches", e))?
                            .into_iter()
                            .map(Ok);

                    let stream: Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>> =
                        Box::pin(stream::iter(flight_data));

                    let res = Response::new(stream);
                    log::info!("response=rows={},bytes={}", num_rows, num_bytes);
                    return Ok(res);
                }
            }
        }

        Err(Status::not_found("no data"))
    }

    async fn do_put_statement_update(
        &self,
        ticket: CommandStatementUpdate,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        log::info!("do_put_statement_update");
        log::info!("ticket={:#?}", ticket);

        Err(Status::unimplemented(
            "do_put_statement_update not implemented",
        ))
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

        Err(Status::unimplemented(
            "do_action_create_prepared_statement not implemented",
        ))
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
