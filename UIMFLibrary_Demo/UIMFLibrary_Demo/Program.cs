using System;
using System.IO;

namespace UIMFLibrary_Demo
{
	class Program
	{
		static void Main(string[] args)
		{

			if (args == null || args.Length == 0)
			{
				Console.WriteLine("Please provide the path to a UIMF file.");
				System.Threading.Thread.Sleep(3000);
				return;
			}

			var runner = new DemoRunner(args[0]);
			runner.Execute();

			WriterTest();

			System.Threading.Thread.Sleep(1000);

		}

		private static void WriterTest()
		{
			var fiTestFile = new FileInfo("TestOutput.uimf");

			try
			{
				if (fiTestFile.Exists)
					fiTestFile.Delete();
			}
			catch (IOException)
			{
				Console.WriteLine("Existing " + fiTestFile.Name + " file exists and cannot be deleted (locked)");
				return;
			}
			catch (Exception ex)
			{
				Console.WriteLine("Existing " + fiTestFile.Name + " file exists and cannot be deleted: " + ex.Message);
				return;
			}

			try
			{
				Console.WriteLine("Creating " + fiTestFile.FullName);

				using (var writer = new UIMFLibrary.DataWriter(fiTestFile.FullName))
				{
					writer.CreateTables();

					var globalParams = new UIMFLibrary.GlobalParameters
					{
						BinWidth = 4,
						TOFIntensityType= "int",
						DateStarted = DateTime.Now.ToString()
					};

					writer.InsertGlobal(globalParams);

					globalParams = writer.GetGlobalParameters();
					writer.UpdateGlobalParameter("TimeOffset", "1");

					writer.UpdateGlobalParameter("Instrument_Name", "IMS_Test");

					Console.WriteLine("Adding frame 1");

					var fp = new UIMFLibrary.FrameParameters
					{
						FragmentationProfile = new double[2],
						FrameNum = 1,
						FrameType = UIMFLibrary.DataReader.FrameType.MS1
					};

					writer.InsertFrame(fp);

					Console.WriteLine("Adding frame 1, scan 1");

					var intensities = new int[599];
					var randGenerator = new Random();

					for (int i = 0; i < intensities.Length; i++)
					{
						intensities[i] = randGenerator.Next(0, 255);
					}

					writer.InsertScan(fp, 1, intensities, globalParams.BinWidth);
				}
			}
			catch (Exception ex)
			{
				Console.WriteLine("Error testing the writer: " + ex.Message);
			}

		}
	}
}
