// --------------------------------------------------------------------------------------------------------------------
// <summary>
//   Get extracted ion chromatograms tests.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace UIMFLibrary.UnitTests.DataReaderTests
{
	using System;
	using System.Diagnostics;
	using System.Text;

	using NUnit.Framework;

	/// <summary>
	/// The get extracted ion chromatograms tests.
	/// </summary>
	[TestFixture]
	public class GetExtractedIonChromatogramsTests
	{
		#region Fields

		/// <summary>
		/// The m_reader.
		/// </summary>
		private DataReader m_reader;

		#endregion

		#region Public Methods and Operators

		/// <summary>
		/// The get 3D elution profile_test 1.
		/// </summary>
		[Test]
		public void Get3DElutionProfile_test1()
		{
            DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

			const int startFrame = 1280;
			const int startScan = 163;
			const double targetMZ = 464.25486;
			const double toleranceInPPM = 25;

			const double toleranceInMZ = toleranceInPPM / 1e6 * targetMZ;

		    int[] intensityVals;

			var sw = new Stopwatch();
			sw.Start();

			using (this.m_reader = new DataReader(FileRefs.uimfStandardFile1))
			{
			    int[] frameVals;
			    int[] scanVals;
			    this.m_reader.Get3DElutionProfile(
					startFrame - 20, 
					startFrame + 20, 
					0, 
					startScan - 20, 
					startScan + 20, 
					targetMZ, 
					toleranceInMZ, 
					out frameVals, 
					out scanVals, 
					out intensityVals);
			}

		    sw.Stop();

			int max = TestUtilities.GetMax(intensityVals);
			var normInten = new float[intensityVals.Length];
			for (int i = 0; i < intensityVals.Length; i++)
			{
				normInten[i] = (float)intensityVals[i] / max;
			}

		    Assert.AreEqual(1913, max);
            Assert.AreEqual((float)0.0172503926, normInten[16]);

			Console.WriteLine("Time (ms) = " + sw.ElapsedMilliseconds);
		}

		/// <summary>
		/// The get 3D elution profile_test 2.
		/// </summary>
		[Test]
		public void Get3DElutionProfile_test2()
		{
            DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

			const int startFrame = 524;
			const int startScan = 128;

			const double targetMZ = 295.9019; // see frame 2130, scan 153
			const double toleranceInPPM = 25;
			const double toleranceInMZ = toleranceInPPM / 1e6 * targetMZ;

			using (this.m_reader = new DataReader(FileRefs.uimfStandardFile1))
			{
				int[][] values = this.m_reader.GetFramesAndScanIntensitiesForAGivenMz(
					startFrame - 40, 
					startFrame + 40, 
					0, 
					startScan - 60, 
					startScan + 60, 
					targetMZ, 
					toleranceInMZ);

				var sb = new StringBuilder();

				foreach (int[] frameIntensities in values)
				{
                    foreach (int scanIntensityValue in frameIntensities)
				    {
                        if (scanIntensityValue > 0)
                            sb.Append(scanIntensityValue + ",");
				    }

				    sb.Append(Environment.NewLine);
				}

			    Assert.AreEqual(293, values[0][64]);
                Assert.AreEqual(510, values[2][66]);
                Assert.AreEqual(663, values[3][64]);
                Assert.AreEqual(436, values[4][57]);
                
				// Console.WriteLine(sb.ToString());
			}
		}

		/// <summary>
		/// The get 3D elution profile_test 3.
		/// </summary>
		[Test]
		public void Get3DElutionProfile_test3()
		{
            DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

			const int startFrame = 400;
			const int stopFrame = 600;

			const int startScan = 110;
			const int stopScan = 210;

			const double targetMZ = 475.7499;
			const double toleranceInPPM = 25;
			const double toleranceInMZ = toleranceInPPM / 1e6 * targetMZ;

			const string filePath = @"\\proto-2\UnitTest_Files\DeconTools_TestFiles\UIMF\Sarc_MS_90_21Aug10_Cheetah_10-08-02_0000.uimf";

			int[] frameVals;
			int[] scanVals;
			int[] intensityVals;

			var sw = new Stopwatch();
			sw.Start();

			using (this.m_reader = new DataReader(filePath))
			{
				this.m_reader.Get3DElutionProfile(
					startFrame, 
					stopFrame, 
					0, 
					startScan, 
					stopScan, 
					targetMZ, 
					toleranceInMZ, 
					out frameVals, 
					out scanVals, 
					out intensityVals);
			}

			sw.Stop();
			Console.WriteLine("Time in millisec for extracting 3D profile = " + sw.ElapsedMilliseconds);

			var sb = new StringBuilder();
			for (int i = 0; i < frameVals.Length; i++)
			{
				sb.Append(frameVals[i] + "\t" + scanVals[i] + "\t" + intensityVals[i] + Environment.NewLine);
			}

			// Console.WriteLine(sb.ToString());
		}

		/// <summary>
		/// The get drift time profile test 1.
		/// </summary>
		[Test]
		public void GetDriftTimeProfileTest1()
		{
            DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

			const int startFrame = 1280;
			const int startScan = 150;
			const double targetMZ = 451.55;
			const double toleranceInPPM = 10;

			const double toleranceInMZ = toleranceInPPM / 1e6 * targetMZ;

			int[] scanVals = null;
			int[] intensityVals = null;

			using (this.m_reader = new DataReader(FileRefs.uimfStandardFile1))
			{
				this.m_reader.GetDriftTimeProfile(
					startFrame - 2, 
					startFrame + 2, 
					DataReader.FrameType.MS1, 
					startScan - 100, 
					startScan + 100, 
					targetMZ, 
					toleranceInMZ, 
					ref scanVals, 
					ref intensityVals);

				TestUtilities.Display2DChromatogram(scanVals, intensityVals);

                Assert.AreEqual(50, scanVals[0]);
                Assert.AreEqual(250, scanVals[200]);

                Assert.AreEqual(6525, intensityVals[100]);
                Assert.AreEqual(3199, intensityVals[105]);
                Assert.AreEqual(255, intensityVals[111]);

			}
		}

		/// <summary>
		/// The get lc chromatogram test 2.
		/// </summary>
		[Test]
		public void GetLCChromatogramTest2()
		{
            DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

			const int startFrame = 600;
			const int endFrame = 800;

			const int startScan = 100;
			const int stopScan = 350;

			using (this.m_reader = new DataReader(FileRefs.uimfStandardFile1))
			{
				const double targetMZ = 636.8466; // see frame 1000, scan 170
				const double toleranceInPPM = 20;
				const double toleranceInMZ = toleranceInPPM / 1e6 * targetMZ;

				int[] frameVals;
				int[] intensityVals;

				// m_reader.GetDriftTimeProfile(testFrame, frameType, startScan, stopScan, targetMZ, toleranceInMZ, ref scanVals, ref intensityVals);
				var sw = new Stopwatch();
				sw.Start();
				this.m_reader.GetLCProfile(
					startFrame, 
					endFrame, 
					DataReader.FrameType.MS1, 
					startScan, 
					stopScan, 
					targetMZ, 
					toleranceInMZ, 
					out frameVals, 
					out intensityVals);
				sw.Stop();

				var sb = new StringBuilder();
				for (int i = 0; i < frameVals.Length; i++)
				{
					sb.Append(frameVals[i]);
					sb.Append('\t');
					sb.Append(intensityVals[i]);
					sb.Append(Environment.NewLine);
				}

				// Assert.AreEqual(171, frameVals[71]);
				// Assert.AreEqual(6770, intensityVals[71]);
				Assert.AreEqual(endFrame - startFrame + 1, frameVals.Length);

				// Console.Write(sb.ToString());
				Console.WriteLine("Time (ms) = " + sw.ElapsedMilliseconds);
			}
		}

		/// <summary>
		/// The get lc chromatogram test 3.
		/// </summary>
		[Test]
		public void GetLCChromatogramTest3()
		{
            DataReaderTests.PrintMethodName(System.Reflection.MethodBase.GetCurrentMethod());

			const int startFrame = 1280;
			const int startScan = 163;
			const double targetMZ = 464.25486;
			const double toleranceInPPM = 25;

			const double toleranceInMZ = toleranceInPPM / 1e6 * targetMZ;
			using (this.m_reader = new DataReader(FileRefs.uimfStandardFile1))
			{
				int[] frameVals;

				// int[] scanVals = null;
				int[] intensityVals;

				var sw = new Stopwatch();
				sw.Start();
				this.m_reader.GetLCProfile(
					startFrame - 200, 
					startFrame + 200, 
					DataReader.FrameType.MS1, 
					startScan - 2, 
					startScan + 2, 
					targetMZ, 
					toleranceInMZ, 
					out frameVals, 
					out intensityVals);
				sw.Stop();

				Console.WriteLine("Time (ms) = " + sw.ElapsedMilliseconds);

				// TestUtilities.display2DChromatogram(frameVals, intensityVals);
			}
		}

		#endregion
	}
}