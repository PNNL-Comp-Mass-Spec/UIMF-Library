// --------------------------------------------------------------------------------------------------------------------
// <copyright file="GetMassSpectrumTests.cs" company="">
//   
// </copyright>
// <summary>
//   TODO The frame and scan info.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace UIMFLibrary.UnitTests.DataReaderTests
{
	using System;
	using System.Linq;

	using NUnit.Framework;

	/// <summary>
	/// TODO The frame and scan info.
	/// </summary>
	internal class FrameAndScanInfo
	{
		#region Fields

		/// <summary>
		/// TODO The start frame.
		/// </summary>
		public int startFrame;

		/// <summary>
		/// TODO The start scan.
		/// </summary>
		public int startScan;

		/// <summary>
		/// TODO The stop frame.
		/// </summary>
		public int stopFrame;

		/// <summary>
		/// TODO The stop scan.
		/// </summary>
		public int stopScan;

		#endregion

		#region Constructors and Destructors

		/// <summary>
		/// Initializes a new instance of the <see cref="FrameAndScanInfo"/> class.
		/// </summary>
		/// <param name="iStartFrame">
		/// TODO The i start frame.
		/// </param>
		/// <param name="iStopFrame">
		/// TODO The i stop frame.
		/// </param>
		/// <param name="iStartScan">
		/// TODO The i start scan.
		/// </param>
		/// <param name="iStopScan">
		/// TODO The i stop scan.
		/// </param>
		public FrameAndScanInfo(int iStartFrame, int iStopFrame, int iStartScan, int iStopScan)
		{
			this.startFrame = iStartFrame;
			this.stopFrame = iStopFrame;
			this.startScan = iStartScan;
			this.stopScan = iStopScan;
		}

		#endregion
	}

	/// <summary>
	/// TODO The get mass spectrum tests.
	/// </summary>
	[TestFixture]
	public class GetMassSpectrumTests
	{
		#region Fields

		/// <summary>
		/// TODO The test frame scan info 1.
		/// </summary>
		private FrameAndScanInfo testFrameScanInfo1 = new FrameAndScanInfo(0, 0, 110, 150);

		#endregion

		#region Public Methods and Operators

		/// <summary>
		/// TODO The get frame 0_ m s_ test 1.
		/// </summary>
		[Test]
		public void getFrame0_MS_Test1()
		{
			using (DataReader dr = new DataReader(FileRefs.uimfStandardFile1))
			{
				GlobalParameters gp = dr.GetGlobalParameters();
				int[] intensities = new int[gp.Bins];
				double[] mzValues = new double[gp.Bins];

				int nonZeros = dr.GetSpectrum(
					this.testFrameScanInfo1.startFrame, 
					this.testFrameScanInfo1.stopFrame, 
					DataReader.FrameType.MS1, 
					this.testFrameScanInfo1.startScan, 
					this.testFrameScanInfo1.stopScan, 
					out mzValues, 
					out intensities);
				TestUtilities.displayRawMassSpectrum(mzValues, intensities);
			}
		}

		/// <summary>
		/// TODO The get frame 0_ m s_demultiplexed data_ test 1.
		/// </summary>
		[Test]
		public void getFrame0_MS_demultiplexedData_Test1()
		{
			using (DataReader dr = new DataReader(FileRefs.uimfStandardDemultiplexedFile1))
			{
				GlobalParameters gp = dr.GetGlobalParameters();
				int[] intensities = new int[gp.Bins];
				double[] mzValues = new double[gp.Bins];

				bool bRunTest = false;
				if (bRunTest)
				{
					int nonZeros = dr.GetSpectrum(
						this.testFrameScanInfo1.startFrame, 
						this.testFrameScanInfo1.stopFrame, 
						DataReader.FrameType.MS1, 
						this.testFrameScanInfo1.startScan, 
						this.testFrameScanInfo1.stopScan, 
						out mzValues, 
						out intensities);
					TestUtilities.displayRawMassSpectrum(mzValues, intensities);
				}
			}
		}

		/// <summary>
		/// TODO The get multiple summed mass spectrums test 1.
		/// </summary>
		[Test]
		public void getMultipleSummedMassSpectrumsTest1()
		{
			using (DataReader dr = new DataReader(FileRefs.uimfStandardFile1))
			{
				FrameAndScanInfo testFrameScanInfo2 = new FrameAndScanInfo(500, 550, 250, 256);

				for (int frame = testFrameScanInfo2.startFrame; frame <= testFrameScanInfo2.stopFrame; frame++)
				{
					GlobalParameters gp = dr.GetGlobalParameters();

					int[] intensities = new int[gp.Bins];
					double[] mzValues = new double[gp.Bins];

					int nonZeros = dr.GetSpectrum(
						frame, 
						frame, 
						DataReader.FrameType.MS1, 
						testFrameScanInfo2.startScan, 
						testFrameScanInfo2.stopScan, 
						out mzValues, 
						out intensities);

					// jump back
					nonZeros = dr.GetSpectrum(
						frame - 1, 
						frame - 1, 
						DataReader.FrameType.MS1, 
						testFrameScanInfo2.startScan, 
						testFrameScanInfo2.stopScan, 
						out mzValues, 
						out intensities);

					// and ahead... just testing it's ability to jump around
					nonZeros = dr.GetSpectrum(
						frame + 2, 
						frame + 2, 
						DataReader.FrameType.MS1, 
						testFrameScanInfo2.startScan, 
						testFrameScanInfo2.stopScan, 
						out mzValues, 
						out intensities);
				}
			}
		}

		/// <summary>
		/// TODO The get single summed mass spectrum test 1.
		/// </summary>
		[Test]
		public void getSingleSummedMassSpectrumTest1()
		{
			using (DataReader dr = new DataReader(FileRefs.uimfStandardFile1))
			{
				GlobalParameters gp = dr.GetGlobalParameters();
				int[] intensities = new int[gp.Bins];
				double[] mzValues = new double[gp.Bins];

				int nonZeros = dr.GetSpectrum(
					this.testFrameScanInfo1.startFrame, 
					this.testFrameScanInfo1.stopFrame, 
					DataReader.FrameType.MS1, 
					this.testFrameScanInfo1.startScan, 
					this.testFrameScanInfo1.stopScan, 
					out mzValues, 
					out intensities);

				int nonZeroCount = (from n in mzValues where n != 0 select n).Count();
				Console.WriteLine("Num xy datapoints = " + nonZeroCount);

				// Assert.AreEqual(0, nonZeros);
			}
		}

		#endregion
	}
}