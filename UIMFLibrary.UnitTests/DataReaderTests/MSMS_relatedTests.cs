// --------------------------------------------------------------------------------------------------------------------
// <copyright file="MSMS_relatedTests.cs" company="">
//   
// </copyright>
// <summary>
//   TODO The msm s_related tests.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace UIMFLibrary.UnitTests.DataReaderTests
{
	using System.Text;

	using NUnit.Framework;

	/// <summary>
	/// TODO The msm s_related tests.
	/// </summary>
	public class MSMS_relatedTests
	{
		#region Public Methods and Operators

		/// <summary>
		/// TODO The get frame type test 1.
		/// </summary>
		[Test]
		public void GetFrameTypeTest1()
		{
			using (var reader = new DataReader(FileRefs.uimfContainingMSMSData1))
			{
				GlobalParameters gp = reader.GetGlobalParameters();

				int checkSum = 0;

				for (int frame = 1; frame <= gp.NumFrames; frame++)
				{
					checkSum += frame * (int)reader.GetFrameTypeForFrame(frame);
				}

				Assert.AreEqual(222, checkSum);
			}
		}

		/// <summary>
		/// TODO The get msms test 1.
		/// </summary>
		[Test]
		public void GetMSMSTest1()
		{
			using (var reader = new DataReader(FileRefs.uimfContainingMSMSData1))
			{
				int testFrame = 2;
				int startScan = 1;
				int stopScan = 300;

				int[] intensityArray;
				double[] mzArray;
				reader.GetSpectrum(
					testFrame, 
					testFrame, 
					DataReader.FrameType.MS2, 
					startScan, 
					stopScan, 
					out mzArray, 
					out intensityArray);

				StringBuilder sb = new StringBuilder();
				for (int i = 0; i < mzArray.Length; i++)
				{
					sb.Append(mzArray[i] + "\t" + intensityArray[i] + "\n");
				}

				Assert.IsNotNull(mzArray);
				Assert.IsTrue(mzArray.Length > 0);
			}
		}

		/// <summary>
		/// TODO The contains msms data test 3.
		/// </summary>
		[Test]
		public void containsMSMSDataTest3()
		{
			using (var reader = new DataReader(FileRefs.uimfContainingMSMSData1))
			{
				Assert.AreEqual(true, reader.HasMSMSData());
			}
		}

		/// <summary>
		/// TODO The contains msms data_test 1.
		/// </summary>
		[Test]
		public void containsMSMSData_test1()
		{
			using (DataReader reader = new DataReader(FileRefs.uimfStandardFile1))
			{
				Assert.AreEqual(false, reader.HasMSMSData());
			}
		}

		/// <summary>
		/// TODO The contains msms data_test 2.
		/// </summary>
		[Test]
		public void containsMSMSData_test2()
		{
			using (var reader = new DataReader(FileRefs.uimfStandardFile1))
			{
				Assert.AreEqual(false, reader.HasMSMSData());
			}
		}

		#endregion
	}
}