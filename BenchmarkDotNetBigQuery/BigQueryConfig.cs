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

        /**
         * Creates a new BigQueryConfig.
         */
        public BigQueryConfig(
            string commitId,
            string googleProjectId,
            string datasetId,
            string summaryTableId = "BenchmarkSummary",
            string reportTableId = "BenchmarkReport",
            GoogleCredential googleCredential = null)
        {
            _bigQueryExporter = new Lazy<BigQueryExporter>(() =>
            {
                return new BigQueryExporter(
                    commitId, googleProjectId, datasetId, summaryTableId,
                    reportTableId, googleCredential);
            });
        }

        /**
         * Returns a BigQueryExporter instantiated with the values from the constructor.
         */
        public IEnumerable<IExporter> GetExporters()
        {
            yield return _bigQueryExporter.Value;
        }

        /**
         * Union with other configs.
         */
        public ConfigUnionRule UnionRule => ConfigUnionRule.Union;

        /**
         * Don't keep benchmark files unless another configs says we should.
         */
        public bool KeepBenchmarkFiles => false;

        /**
         * Null.
         */
        public IOrderProvider GetOrderProvider() => null;

        /**
         * Empty.
         */
        public IEnumerable<IColumnProvider> GetColumnProviders() => Enumerable.Empty<IColumnProvider>();

        /**
         * Empty.
         */
        public IEnumerable<ILogger> GetLoggers() => Enumerable.Empty<ILogger>();

        /**
         * Empty.
         */
        public IEnumerable<IDiagnoser> GetDiagnosers() => Enumerable.Empty<IDiagnoser>();

        /**
         * Empty.
         */
        public IEnumerable<IAnalyser> GetAnalysers() => Enumerable.Empty<IAnalyser>();

        /**
         * Empty.
         */
        public IEnumerable<Job> GetJobs() => Enumerable.Empty<Job>();

        /**
         * Empty.
         */
        public IEnumerable<IValidator> GetValidators() => Enumerable.Empty<IValidator>();
    }
}
