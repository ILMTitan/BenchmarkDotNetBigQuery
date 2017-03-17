using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Reports;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;

namespace BenchmarkDotNetBigQuery
{
    /**
     * A BenchmarkDotNet Exporter that saves benchmark data to Google BigQuery Tables.
     */
    public class BigQueryExporter: IExporter
    {
        private string CommitId { get; }
        private BigQueryTable SummaryTable { get; }
        private BigQueryTable ReportTable { get; }

        private readonly TableSchema _summaryTableSchema = new TableSchemaBuilder
        {
            {"Id", BigQueryDbType.String},
            {"Commit", BigQueryDbType.String},
            {"Timestamp", BigQueryDbType.Timestamp},
            {"HostName", BigQueryDbType.String},
            {"OsVersion", BigQueryDbType.String},
            {"ProcessorName", BigQueryDbType.String},
            {"ProcessorCount", BigQueryDbType.Int64},
            {"RuntimeVersion", BigQueryDbType.String},
            {"Architecture", BigQueryDbType.String},
            {"JitModules", BigQueryDbType.String},
            {"DotNetCoreVersion", BigQueryDbType.String},
            {"BenchmarkDotNetVersion", BigQueryDbType.String},
            {"ChronometerFrequency", BigQueryDbType.Int64},
            {"HardwareTimerKind", BigQueryDbType.String}
        }.Build();

        private readonly TableSchema _reportTableSchema = new TableSchemaBuilder
        {
            {"SummaryId", BigQueryDbType.String},
            {"Namespace", BigQueryDbType.String},
            {"Type", BigQueryDbType.String},
            {"FullType", BigQueryDbType.String},
            {"MethodName", BigQueryDbType.String},
            {"FullMethodName", BigQueryDbType.String},
            {"Parameters", BigQueryDbType.String},
            {"MethodSigniture", BigQueryDbType.String},
            {"Min", BigQueryDbType.Float64},
            {"Max", BigQueryDbType.Float64},
            {"Median", BigQueryDbType.Float64},
            {"StandardDeviation", BigQueryDbType.Float64},
            {"StandardError", BigQueryDbType.Float64},
            {"Variance", BigQueryDbType.Float64},
            {"Percentile67", BigQueryDbType.Float64},
            {"Percentile85", BigQueryDbType.Float64},
            {"Percentile95", BigQueryDbType.Float64},
            {"Percentile100", BigQueryDbType.Float64}
        }.Build();

        /**
         * During construction, the BigQueryExporter will create the dataset and tables if needed.
         * If the dataset and tables already exist, it will validate that the tables contain the necessary fields.
         */
        public BigQueryExporter(
            string commitId,
            string googleProjectId,
            string datasetId,
            string summaryTableId = "BenchmarkSummary",
            string reportTableId = "BenchmarkReport",
            GoogleCredential googleCredential = null)
        {
            CommitId = commitId;
            Task<BigQueryClient> bqClientTask = BigQueryClient.CreateAsync(googleProjectId, googleCredential);
            Tuple<BigQueryTable, BigQueryTable> tables =
                GetValidTablesFromDataset(datasetId, summaryTableId, reportTableId, bqClientTask).Result;
            SummaryTable = tables.Item1;
            ReportTable = tables.Item2;
        }

        /**
         * BigQueryExporter does not write to a logger. It sends an error message to the logger.
         */
        public void ExportToLog(Summary summary, ILogger logger)
        {
            logger.WriteLine(LogKind.Error, $"{nameof(BigQueryExporter)} does not output to a logger.");
        }

        /**
         * This is where BigQueryExporter writes benchmark data to the BigQuery tables.
         */
        public IEnumerable<string> ExportToFiles(Summary summary, ILogger logger)
        {
            string summaryId = Guid.NewGuid().ToString();
            var summaryRow = BuildSummaryRow(summary, summaryId);
            Task insertSummaryTask = SummaryTable.InsertAsync(summaryRow);
            IEnumerable<BigQueryInsertRow> reportRows = summary.Reports.Select(BuildReportRowCurried(summaryId));
            Task insertReportTask = ReportTable.InsertAsync(reportRows);
            Task.WaitAll(insertSummaryTask, insertReportTask);
            yield return $"{summaryId} in {SummaryTable.FullyQualifiedId} and {ReportTable.FullyQualifiedId}";
        }

        private BigQueryInsertRow BuildSummaryRow(Summary summary, string summaryId)
        {
            return new BigQueryInsertRow
            {
                {"Id", summaryId},
                {"Commit", CommitId},
                {"Timestamp", DateTimeOffset.UtcNow},
                {"HostName", Environment.MachineName},
                {"OsVersion", summary.HostEnvironmentInfo.OsVersion.Value},
                {"ProcessorName", summary.HostEnvironmentInfo.ProcessorName.Value},
                {"ProcessorCount", summary.HostEnvironmentInfo.ProcessorCount},
                {"RuntimeVersion", summary.HostEnvironmentInfo.RuntimeVersion},
                {"Architecture", summary.HostEnvironmentInfo.Architecture},
                {"JitModules", summary.HostEnvironmentInfo.JitModules},
                {"DotNetCoreVersion", summary.HostEnvironmentInfo.DotNetCliVersion.Value},
                {"BenchmarkDotNetVersion", summary.HostEnvironmentInfo.BenchmarkDotNetVersion},
                {"ChronometerFrequency", summary.HostEnvironmentInfo.ChronometerFrequency.Hertz},
                {"HardwareTimerKind", summary.HostEnvironmentInfo.HardwareTimerKind.ToString()}
            };
        }

        private static Func<BenchmarkReport, BigQueryInsertRow> BuildReportRowCurried(string summaryId)
        {
            return (report) => BuildReportRow(summaryId, report);
        }

        private static BigQueryInsertRow BuildReportRow(string summaryId, BenchmarkReport report)
        {
            var fullMethodName = $"{report.Benchmark.Target.Type.FullName}.{report.Benchmark.Target.Method.Name}";
            return new BigQueryInsertRow
            {
                {"SummaryId", summaryId},
                {"Namespace", report.Benchmark.Target.Type.Namespace},
                {"Type", report.Benchmark.Target.Type.Name},
                {"FullType", report.Benchmark.Target.Type.FullName},
                {"MethodName", report.Benchmark.Target.Method.Name},
                {"FullMethodName", fullMethodName},
                {"Parameters", report.Benchmark.Parameters.PrintInfo},
                {"MethodSigniture", report.Benchmark.Target.Method.ToString()},
                {"Min", report.ResultStatistics.Min},
                {"Max", report.ResultStatistics.Max},
                {"Median", report.ResultStatistics.Median},
                {"StandardDeviation", report.ResultStatistics.StandardDeviation},
                {"StandardError", report.ResultStatistics.StandardError},
                {"Variance", report.ResultStatistics.Variance},
                {"Percentile67", report.ResultStatistics.Percentiles.P67},
                {"Percentile85", report.ResultStatistics.Percentiles.P85},
                {"Percentile95", report.ResultStatistics.Percentiles.P95},
                {"Percentile100", report.ResultStatistics.Percentiles.P100}
            };
        }

        private async Task<Tuple<BigQueryTable, BigQueryTable>> GetValidTablesFromDataset(
            string datasetId, string summaryTableId, string reportTableId, Task<BigQueryClient> bigQueryClientTask)
        {
            BigQueryClient bigQueryClient = await bigQueryClientTask;
            BigQueryDataset dataset = await bigQueryClient.GetOrCreateDatasetAsync(datasetId);

            Task<BigQueryTable> summaryTableTask = dataset.GetOrCreateTableAsync(summaryTableId, _summaryTableSchema);
            Task<BigQueryTable> reportTableTask = dataset.GetOrCreateTableAsync(reportTableId, _reportTableSchema);

            Task validateSummaryTableTask = ValidateTableSchemaAsync(_summaryTableSchema, summaryTableTask);
            Task validateReportTableTask = ValidateTableSchemaAsync(_reportTableSchema, reportTableTask);

            await Task.WhenAll(validateSummaryTableTask, validateReportTableTask);
            return Tuple.Create(await summaryTableTask, await reportTableTask);
        }

        private async Task ValidateTableSchemaAsync(TableSchema testSchema, Task<BigQueryTable> tableTask)
        {
            BigQueryTable actualTable = await tableTask;
            ValidateSchema(testSchema.Fields, actualTable.Schema.Fields, actualTable.Reference.TableId);
        }

        private void ValidateSchema(
            IList<TableFieldSchema> testSchema, IList<TableFieldSchema> actualSchema, string schemaId)
        {
            int actualCount = actualSchema?.Count ?? 0;
            int testCount = testSchema?.Count ?? 0;
            if (actualCount < testCount)
            {
                throw new InvalidOperationException($"Schema for {schemaId} has too few fields.");
            }
            if (testSchema != null && actualSchema != null)
            {
                Func<TableFieldSchema, string> fieldNameSelector = schema => schema.Name;
                var fields =
                    testSchema.GroupJoin(actualSchema, fieldNameSelector, fieldNameSelector, Tuple.Create);
                foreach (Tuple<TableFieldSchema, IEnumerable<TableFieldSchema>> fieldTuple in fields)
                {
                    TableFieldSchema testField = fieldTuple.Item1;
                    IEnumerable<TableFieldSchema> actualFields = fieldTuple.Item2;
                    ValidateFieldSchema(testField, actualFields, $"{schemaId}.{testField.Name}");
                }
            }
        }

        private void ValidateFieldSchema(
            TableFieldSchema testField, IEnumerable<TableFieldSchema> actualFields, string fieldSchemaId)
        {
            TableFieldSchema actualField;
            try
            {
                actualField = actualFields.Single();
            }
            catch (Exception e)
            {
                throw new InvalidOperationException(
                    $"Field {fieldSchemaId} does not exist exactly once on actual table.", e);
            }
            ValidateSchema(testField.Fields, actualField.Fields, fieldSchemaId);
            if (!testField.Type.Equals(actualField.Type))
            {
                throw new InvalidOperationException(
                    $"Field {fieldSchemaId} had Type {actualField.Type} but should have {testField.Type}");
            }
        }
    }
}
