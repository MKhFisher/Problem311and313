using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Problem311and313
{
    public class Frequency
    {
        public string term { get; set; }
        public int frequency { get; set; }
    }

    public class FreqList
    {
        public List<Frequency> freq_list = new List<Frequency>();
        public FreqCollection result;
    }

    public class FreqCollection
    {
        public ConcurrentDictionary<string, int> list = new ConcurrentDictionary<string, int>();
        public ConcurrentBag<Frequency> collection = new ConcurrentBag<Frequency>();
    }

    public class LinesList
    {
        public List<string> lines;
        public FreqCollection list;
        //public LinesList parent;
        //public ConcurrentDictionary<Thread, List<Frequency>> local_list = new ConcurrentDictionary<Thread, List<Frequency>>();
        public static HashSet<string> stopwords;
    }

    class Program
    {
        static void Main(string[] args)
        {
            FreqCollection result = new FreqCollection();
            List<string> text = Partition(args[0]);
            List<string> temp = new List<string>();

            List<List<Frequency>> final_groups = new List<List<Frequency>>();
            List<Frequency> A_E = new List<Frequency>();
            List<Frequency> F_J = new List<Frequency>();
            List<Frequency> K_O = new List<Frequency>();
            List<Frequency> P_T = new List<Frequency>();
            List<Frequency> U_Z = new List<Frequency>();
            final_groups.Add(A_E);
            final_groups.Add(F_J);
            final_groups.Add(K_O);
            final_groups.Add(P_T);
            final_groups.Add(U_Z);

            LinesList.stopwords = GetStopWords("stop_words.txt");
            List<Thread> threads = new List<Thread>();

            for (int i = 0; i < text.Count; i++)
            {
                temp.Add(text[i]);

                if (temp.Count == 1000 || i == text.Count - 1)
                {
                    LinesList line = new LinesList { lines = temp};
                    line.list = result;
                    Thread t = new Thread(SplitLines);

                    t.Start(line);
                    threads.Add(t);
                    temp = new List<string>();
                }
            }

            foreach (Thread t in threads)
            {
                t.Join();
            }

            foreach (Frequency f in result.collection)
            {
                if (f.term.StartsWith("a") || f.term.StartsWith("b") || f.term.StartsWith("c") || f.term.StartsWith("d") || f.term.StartsWith("e"))
                {
                    A_E.Add(f);
                }
            }

            threads.Clear();

            foreach (List<Frequency> freqs in final_groups)
            {
                FreqList fl = new FreqList();
                fl.freq_list = freqs;
                fl.result = result;

                Thread t = new Thread(MergeFreqs);
                t.Start(fl);

                threads.Add(t);
            }

            foreach (Thread t in threads)
            {
                t.Join();
            }

            List<Frequency> final_freqs = new List<Frequency>();
            foreach (KeyValuePair<string, int> kvp in result.list)
            {
                final_freqs.Add(new Frequency { term = kvp.Key, frequency = kvp.Value });
            }

            final_freqs = final_freqs.OrderByDescending(x => x.frequency).Take(25).ToList();

            foreach (Frequency f in final_freqs)
            {
                Console.WriteLine("{0}  -  {1}", f.term, f.frequency);
            }

            Console.ReadKey();
        }

        static void MergeFreqs(Object freqs)
        {
            FreqList result = (FreqList)freqs;

            foreach (Frequency f in result.freq_list)
            {
                if (result.result.list.ContainsKey(f.term))
                {
                    result.result.list[f.term] += 1;
                }
                else
                {
                    result.result.list[f.term] = 1;
                }
            }
        }

        public static HashSet<string> GetStopWords(string file)
        {
            return new HashSet<string>(new StreamReader(file).ReadToEnd().Replace(",\n\n", "").ToLower().Split(','));   
        }

        static void SplitLines(Object lines)
        {
            LinesList current_lines = (LinesList)lines;
            List<Thread> threads = new List<Thread>();

            for (int i = 0; i < current_lines.lines.Count; i++)
            {
                LinesList line = new LinesList();
                line.lines = new List<string>();
                line.lines.Add(current_lines.lines[i]);
                line.list = current_lines.list;
                //line.parent = current_lines;

                Thread t = new Thread(CountWords);

                t.Start(line);
                threads.Add(t);    
            }

            foreach (Thread t in threads)
            {
                t.Join();
            }
        }

        static void CountWords(Object line)
        {
            LinesList current_line = (LinesList)line;
            List<string> words = Regex.Split(current_line.lines[0], "\\W+").ToList();

            Dictionary<string, int> local_freqs = new Dictionary<string,int>();

            foreach (string word in words)
            {                
                if (!LinesList.stopwords.Contains(word) && word != "s")
                {
                    if (local_freqs.ContainsKey(word))
                    {
                        local_freqs[word] += 1;
                    }
                    else
                    {
                        local_freqs[word] = 1;
                    }
                }
            }

            foreach (KeyValuePair<string, int> kvp in local_freqs)
            {
                current_line.list.collection.Add(new Frequency { term = kvp.Key, frequency = kvp.Value });
            }
        }


        public static List<string> Partition(string file)
        {
            return new StreamReader(file).ReadToEnd().Replace('_', ' ').ToLower().Split('\n').ToList();
        }
    }
}
