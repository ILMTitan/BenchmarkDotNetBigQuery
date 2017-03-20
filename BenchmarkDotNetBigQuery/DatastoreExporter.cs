using System;
using System.Collections.Generic;
using System.Linq;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Reports;
using Google.Cloud.Datastore.V1;
using MoreLinq;

namespace BenchmarkDotNetBigQuery
{
    /// <summary>
    /// A BenchmarkDotNet exporter that exports to Google Cloud Datastore.
    /// </summary>
    public class DatastoreExporter : IExporter
    {
        private Key _summaryKey;
        private bool OneSummary { get; }
        private string CommitId { get; }
        private string SummaryEntityKind { get; }
        private string ReportEntityKind { get; }
        private DatastoreDb DatastoreDb { get; }

        /// <summary>
        /// During construction, the BigQueryExporter will create the dataset and tables if needed.
        /// If the dataset and tables already exist, it will validate that the tables contain the necessary fields.
        /// </summary>
        /// <param name="commitId">Id of the commit e.g. git hash.</param>
        /// <param name="googleProjectId">The id of the google project to upload to.</param>
        /// <param name="oneSummary">
        ///   BenchmarkDotNet provides a separate summary for every class. If this parameter is
        ///   false, the exporter creates all summary entities. If true, creates a single summary entity for the entire
        ///   lifetime of the exporter.
        /// </param>
        /// <param name="gcdNamespace">
        ///   The name of the Google Cloud Datastore Namespace to place the entities in.
        /// </param>
        /// <param name="summaryEntityKind">The kind of entity to put summary information in.</param>
        /// <param name="reportEntityKind">
        ///   The kind of entity to put report information in.
        ///   Report entities will always be children of a summary entity
        /// </param>
        public DatastoreExporter(
            string commitId,
            string googleProjectId,
            bool oneSummary = true,
            string gcdNamespace = "",
            string summaryEntityKind = "BenchmarkSummary",
            string reportEntityKind = "BenchmarkReport")
        {
            CommitId = commitId;
            OneSummary = oneSummary;
            SummaryEntityKind = summaryEntityKind;
            ReportEntityKind = reportEntityKind;
            DatastoreDb = DatastoreDb.Create(googleProjectId, gcdNamespace);
        }


        /// <summary>
        /// DatastoreExporter does not write to a logger. It will send an error message to the logger.
        /// </summary>
        public void ExportToLog(Summary summary, ILogger logger)
        {
            logger.WriteLine(LogKind.Error, $"{nameof(DatastoreExporter)} does not output to a logger.");
        }

        ///<summary>
        /// This is where DatastoreExporter writes benchmark data to Datastore entities.
        /// </summary>
        /// <param name="summary">The summary to export to Google Cloud Datastore.</param>
        /// <param name="consoleLogger">Unused</param>
        /// <returns>A string specifiying the key of the summary entity.</returns>
        public IEnumerable<string> ExportToFiles(Summary summary, ILogger consoleLogger)
        {
            KeyFactory summaryKeyFactory = DatastoreDb.CreateKeyFactory(SummaryEntityKind);
            if (!OneSummary || _summaryKey == null)
            {
                Entity summaryEntity = BuildSummaryEntity(summary, summaryKeyFactory);
                _summaryKey = DatastoreDb.Insert(summaryEntity);
                yield return $"Datastore summary entity key: {_summaryKey}";
            }
            KeyFactory reportKeyFactory = new KeyFactory(_summaryKey, ReportEntityKind);
            var reportBatches = summary.Reports.Select(BuildReportEntityCurry(reportKeyFactory)).Batch(500);
            foreach (IEnumerable<Entity> reportBatch in reportBatches)
            {
                DatastoreDb.Insert(reportBatch);
            }
        }

        private Func<BenchmarkReport, Entity> BuildReportEntityCurry(KeyFactory reportKeyFactory)
        {
            return (report) => BuildReportEntity(report, reportKeyFactory);
        }

        private Entity BuildReportEntity(BenchmarkReport report, KeyFactory reportKeyFactory)
        {
            var fullMethodName = $"{report.Benchmark.Target.Type.FullName}.{report.Benchmark.Target.Method.Name}";
            return new Entity
            {
                Key = reportKeyFactory.CreateIncompleteKey(),
                ["Namespace"] = report.Benchmark.Target.Type.Namespace,
                ["Type"] = report.Benchmark.Target.Type.Name,
                ["FullType"] = report.Benchmark.Target.Type.FullName,
                ["MethodName"] = report.Benchmark.Target.Method.Name,
                ["FullMethodName"] = fullMethodName,
                ["Parameters"] = report.Benchmark.Parameters.PrintInfo,
                ["MethodSignature"] = report.Benchmark.Target.Method.ToString(),
                ["Min"] = report.ResultStatistics.Min,
                ["Max"] = report.ResultStatistics.Max,
                ["Median"] = report.ResultStatistics.Median,
                ["Mean"] = report.ResultStatistics.Mean,
                ["StandardDeviation"] = report.ResultStatistics.StandardDeviation,
                ["StandardError"] = report.ResultStatistics.StandardError,
                ["Variance"] = report.ResultStatistics.Variance,
                ["Percentile67"] = report.ResultStatistics.Percentiles.P67,
                ["Percentile85"] = report.ResultStatistics.Percentiles.P85,
                ["Percentile95"] = report.ResultStatistics.Percentiles.P95,
                ["Percentile100"] = report.ResultStatistics.Percentiles.P100
            };
        }

        private Entity BuildSummaryEntity(Summary summary, KeyFactory summaryKeyFactory)
        {
            return new Entity
            {
                Key = summaryKeyFactory.CreateIncompleteKey(),
                ["Commit"] = CommitId,
                ["Timestamp"] = DateTimeOffset.UtcNow,
                ["HostName"] = Environment.MachineName,
                ["OsVersion"] = summary.HostEnvironmentInfo.OsVersion.Value,
                ["ProcessorName"] = summary.HostEnvironmentInfo.ProcessorName.Value,
                ["ProcessorCount"] = summary.HostEnvironmentInfo.ProcessorCount,
                ["RuntimeVersion"] = summary.HostEnvironmentInfo.RuntimeVersion,
                ["Architecture"] = summary.HostEnvironmentInfo.Architecture,
                ["JitModules"] = summary.HostEnvironmentInfo.JitModules,
                ["DotNetCoreVersion"] = summary.HostEnvironmentInfo.DotNetCliVersion.Value,
                ["BenchmarkDotNetVersion"] = summary.HostEnvironmentInfo.BenchmarkDotNetVersion,
                ["ChronometerFrequency"] = summary.HostEnvironmentInfo.ChronometerFrequency.Hertz,
                ["HardwareTimerKind"] = summary.HostEnvironmentInfo.HardwareTimerKind.ToString()
            };
        }
    }
}
