using System;
using System.Xml;
using Utilities;

namespace SnapperExperimentProcess
{
    public static class WorkloadParser
    {
        public static WorkloadGroup GetSnapperSmallBankWorkLoadFromXML(int experimentID)
        {
            var path = Constants.dataPath + $@"XML\Fig{experimentID}-Snapper.xml";
            var xmlDoc = new XmlDocument();
            xmlDoc.Load(path);
            var rootNode = xmlDoc.DocumentElement;

            var workload = new WorkloadGroup();
            workload.pactPercent = Array.ConvertAll(rootNode.SelectSingleNode("pactPercent").FirstChild.Value.Split(","), x => int.Parse(x));

            workload.txnSize = Array.ConvertAll(rootNode.SelectSingleNode("txnSize").FirstChild.Value.Split(","), x => int.Parse(x));
            workload.numWriter = Array.ConvertAll(rootNode.SelectSingleNode("numWriter").FirstChild.Value.Split(","), x => int.Parse(x));
            workload.distribution = Array.ConvertAll(rootNode.SelectSingleNode("distribution").FirstChild.Value.Split(","), x => Enum.Parse<Distribution>(x));
            workload.zipfianConstant = Array.ConvertAll(rootNode.SelectSingleNode("zipfianConstant").FirstChild.Value.Split(","), x => double.Parse(x));

            workload.actPipeSize = Array.ConvertAll(rootNode.SelectSingleNode("actPipeSize").FirstChild.Value.Split(","), x => int.Parse(x));
            workload.pactPipeSize = Array.ConvertAll(rootNode.SelectSingleNode("pactPipeSize").FirstChild.Value.Split(","), x => int.Parse(x));

            workload.noDeadlock = new bool[] { false };

            return workload;
        }

        public static WorkloadGroup GetSnapperTPCCWorkLoadFromXML(int experimentID)
        {
            var path = Constants.dataPath + $@"XML\Fig{experimentID}-Snapper-TPCC.xml";
            var xmlDoc = new XmlDocument();
            xmlDoc.Load(path);
            var rootNode = xmlDoc.DocumentElement;

            var workload = new WorkloadGroup();
            workload.pactPercent = Array.ConvertAll(rootNode.SelectSingleNode("pactPercent").FirstChild.Value.Split(","), x => int.Parse(x));

            workload.txnSize = new int[] { 0 };
            workload.numWriter = new int[] { 0 };
            workload.distribution = new Distribution[] { Distribution.UNIFORM };
            workload.zipfianConstant = new double[] { 0 };

            workload.actPipeSize = Array.ConvertAll(rootNode.SelectSingleNode("actPipeSize").FirstChild.Value.Split(","), x => int.Parse(x));
            workload.pactPipeSize = Array.ConvertAll(rootNode.SelectSingleNode("pactPipeSize").FirstChild.Value.Split(","), x => int.Parse(x));

            workload.noDeadlock = new bool[] { false };

            return workload;
        }

        public static WorkloadGroup GetNTTPCCWorkloadFromXML(int experimentID)
        {
            var path = Constants.dataPath + $@"XML\Fig{experimentID}-NT-TPCC.xml";
            var xmlDoc = new XmlDocument();
            xmlDoc.Load(path);
            var rootNode = xmlDoc.DocumentElement;

            var workload = new WorkloadGroup();
            workload.pactPercent = new int[] { 0 };

            workload.txnSize = new int[] { 0 };
            workload.numWriter = new int[] { 0 };
            workload.distribution = new Distribution[] { Distribution.UNIFORM};
            workload.zipfianConstant = new double[] { 0 };

            workload.actPipeSize = Array.ConvertAll(rootNode.SelectSingleNode("actPipeSize").FirstChild.Value.Split(","), x => int.Parse(x));
            workload.pactPipeSize = new int[workload.actPipeSize.Length];

            workload.noDeadlock = new bool[] { false };

            return workload;
        }

        public static WorkloadGroup GetNTSmallBankWorkloadFromXML(int experimentID)
        {
            var path = Constants.dataPath + $@"XML\Fig{experimentID}-NT.xml";
            var xmlDoc = new XmlDocument();
            xmlDoc.Load(path);
            var rootNode = xmlDoc.DocumentElement;

            var workload = new WorkloadGroup();
            workload.pactPercent = new int[] { 0 };

            workload.txnSize = Array.ConvertAll(rootNode.SelectSingleNode("txnSize").FirstChild.Value.Split(","), x => int.Parse(x));
            workload.numWriter = Array.ConvertAll(rootNode.SelectSingleNode("numWriter").FirstChild.Value.Split(","), x => int.Parse(x));
            workload.distribution = Array.ConvertAll(rootNode.SelectSingleNode("distribution").FirstChild.Value.Split(","), x => Enum.Parse<Distribution>(x));
            workload.zipfianConstant = Array.ConvertAll(rootNode.SelectSingleNode("zipfianConstant").FirstChild.Value.Split(","), x => double.Parse(x));

            workload.actPipeSize = Array.ConvertAll(rootNode.SelectSingleNode("actPipeSize").FirstChild.Value.Split(","), x => int.Parse(x));
            workload.pactPipeSize = new int[workload.actPipeSize.Length];

            workload.noDeadlock = new bool[] { false };

            return workload;
        }

        public static WorkloadGroup GetOrleansWorkloadFromXML(int experimentID)
        {
            var path = Constants.dataPath + $@"XML\Fig{experimentID}-Orleans.xml";
            var xmlDoc = new XmlDocument();
            xmlDoc.Load(path);
            var rootNode = xmlDoc.DocumentElement;

            var workload = new WorkloadGroup();
            workload.pactPercent = new int[] { 0 };

            workload.txnSize = Array.ConvertAll(rootNode.SelectSingleNode("txnSize").FirstChild.Value.Split(","), x => int.Parse(x));
            workload.numWriter = Array.ConvertAll(rootNode.SelectSingleNode("numWriter").FirstChild.Value.Split(","), x => int.Parse(x));

            workload.distribution = Array.ConvertAll(rootNode.SelectSingleNode("distribution").FirstChild.Value.Split(","), x => Enum.Parse<Distribution>(x));
            workload.zipfianConstant = Array.ConvertAll(rootNode.SelectSingleNode("zipfianConstant").FirstChild.Value.Split(","), x => double.Parse(x));
            workload.actPipeSize = Array.ConvertAll(rootNode.SelectSingleNode("actPipeSize").FirstChild.Value.Split(","), x => int.Parse(x));
            workload.pactPipeSize = new int[workload.actPipeSize.Length];

            workload.noDeadlock = Array.ConvertAll(rootNode.SelectSingleNode("noDeadlock").FirstChild.Value.Split(","), x => bool.Parse(x));

            return workload;
        }
    }
}
