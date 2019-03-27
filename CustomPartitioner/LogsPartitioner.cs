using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace CustomPartitioner
{
    public class LogsPartitioner : Partitioner<string>
    {
        private IEnumerable<string> lines;
        private readonly StringBuilder parseStringBuilder = new StringBuilder();
        private readonly Regex datePrefixRE = new Regex(@"^\d{4}-\d{2}-\d{2}");

        public LogsPartitioner(string filePath)
        {
            lines = File.ReadLines(filePath);
        }
        
        public override IList<IEnumerator<string>> GetPartitions(int partitionCount)
        {
            if (partitionCount <= 0)
                throw new ArgumentOutOfRangeException(nameof(partitionCount) + "must be bigger 0");
            var partitions = new List<string>[partitionCount];
            InitList(partitionCount, partitions);
            using (var linesEnumerator = lines.GetEnumerator())
            {
                var chunkSize = 1;
                var readAll = false;
                while (!readAll)
                {
                    foreach (var partition in partitions)
                    {
                        for (var i = 0; i < chunkSize; i++)
                        {
                            if (!linesEnumerator.MoveNext())
                            {
                                readAll = true;
                                break;
                            }
                            partition.Add(ParseCurrentString(linesEnumerator));
                        }

                        if (readAll)
                            break;
                    }

                    chunkSize++;
                }
            }

            return partitions.Select(list => list.AsEnumerable().GetEnumerator()).ToList();
        }

        private string ParseCurrentString(IEnumerator<string> linesEnumerator)
        {
            var str = linesEnumerator.Current;
            if (str == null)
                throw new NullReferenceException("Enumerator.Current is null");
            if (datePrefixRE.IsMatch(str))
                return str;
            parseStringBuilder.Clear();
            parseStringBuilder.Append(str);
            var currentString = str;
            while (linesEnumerator.MoveNext() && !currentString.Substring(0, 6).Equals("   ---"))
            {
                currentString = linesEnumerator.Current;
                parseStringBuilder.Append(currentString + '\n');
            }

            return parseStringBuilder.ToString();
        }

        private void InitList(int size, List<string>[] list)
        {
            for (var i = 0; i < size; i++)
                list[i] = new List<string>();
        }
    }
}