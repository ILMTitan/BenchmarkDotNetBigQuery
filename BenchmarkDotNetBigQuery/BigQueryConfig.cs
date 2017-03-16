using System;
using System.Collections.Generic;
using System.Linq;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Order;
using BenchmarkDotNet.Validators;
using Google.Apis.Auth.OAuth2;

namespace BenchmarkDotNetBigQuery
{
    /**
     * An IConfig that contains a BigQueryExporter.
     */
    public class BigQueryConfig : IConfig
    {
        private readonly Lazy<BigQueryExporter> _bigQueryExporter;

        public BigQueryConfig(
            string commitId,
            string googleProjectId,
            string datasetId,
            string reportTableId = null,
            string summaryTableId = null,
            GoogleCredential googleCredential = null)
        {
            _bigQueryExporter = new Lazy<BigQueryExporter>(() =>
            {
                return new BigQueryExporter(
                    commitId, googleProjectId, datasetId, summaryTableId,
                    reportTableId, googleCredential);
            });
        }

        public IEnumerable<IExporter> GetExporters()
        {
            yield return _bigQueryExporter.Value;
        }

        public ConfigUnionRule UnionRule => ConfigUnionRule.Union;
        public bool KeepBenchmarkFiles => false;
        public IOrderProvider GetOrderProvider() => null;

        public IEnumerable<IColumnProvider> GetColumnProviders() => Enumerable.Empty<IColumnProvider>();
        public IEnumerable<ILogger> GetLoggers() => Enumerable.Empty<ILogger>();
        public IEnumerable<IDiagnoser> GetDiagnosers() => Enumerable.Empty<IDiagnoser>();
        public IEnumerable<IAnalyser> GetAnalysers() => Enumerable.Empty<IAnalyser>();
        public IEnumerable<Job> GetJobs() => Enumerable.Empty<Job>();
        public IEnumerable<IValidator> GetValidators() => Enumerable.Empty<IValidator>();
    }
}
