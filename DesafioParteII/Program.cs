using Confluent.Kafka;
using System;
using System.IO;
using System.Text;
using System.Threading;

namespace DesafioParteII
{
    public class Program
    {
        public static string[] LerArquivo(string moeda)
        {
            string[] campos;
            string[] NotFound = { "Nada encontrado " };
            try
            {
                string[] linhas = File.ReadAllLines("C:\\DesafioII\\DadosCotacao.csv");
                for (int i = 0; i < linhas.Length; i++)
                {

                    campos = linhas[i].Split(',');

                    if (moeda == campos[4])
                    {
                        MontaArquivo(campos[4], campos[3], campos[0]);
                    }

                    return campos;
                }

                return NotFound;
            }
            catch (Exception)
            {
                return NotFound;
            }

        }

        public static void MontaArquivo(string id_moeda, string Data_Ref, string valor)
        {
            try
            {
                using (StreamWriter file = new StreamWriter("C:\\DesafioII\\Resultado_aaaammdd_HHmmss.csv", true))
                {
                    file.WriteLine(id_moeda + "," + Data_Ref + "," + valor);
                }
            }
            catch (Exception ex)
            {

                throw new ApplicationException("", ex);
            }
        }

        public static void Main(string[] args)
        {
            Program program = new Program();

            var conf = new ConsumerConfig
            {
                GroupId = "test-consumer-group",
                BootstrapServers = "127.0.0.1:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using var c = new ConsumerBuilder<Ignore, string>(conf).Build();

            {
                string moeda;
                string data_ref;
                c.Subscribe("test-topic");

                var cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true;
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = c.Consume(cts.Token);
                            Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                            moeda = cr.Value.ToString().Replace(":", "").Replace("\"", "");
                            data_ref = cr.Value.ToString().Replace(":", "").Replace("\"", "");
                            LerArquivo(moeda.Substring(10, 3));
                            Thread.Sleep(120000);
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    c.Close();
                }
            }
        }
    }
}