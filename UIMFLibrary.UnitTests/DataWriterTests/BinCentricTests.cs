using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace UIMFLibrary.UnitTests.DataWriterTests
{
	public class BinCentricTests
	{
		[Test]
		public void TestCreateBinCentricTables()
		{
			try
			{
				string fileLocation = @"..\..\..\TestFiles\SarcCtrl_P21_1mgml_IMS6_AgTOF07_210min_CID_01_05Oct12_Frodo_Precursors_Removed_Collision_Energy_Collapsed.UIMF";
				FileInfo uimfFile = new FileInfo(fileLocation);

				using (DataWriter uimfWriter = new DataWriter(uimfFile.FullName))
				{
					uimfWriter.CreateBinCentricTables();
				}
			}
			catch (Exception)
			{
				
				
			}

		}

		[Test]
		public void TestCreateBinCentricTablesSmallFile()
		{
			try
			{
				string fileLocation = @"..\..\..\TestFiles\PepMix_MSMS_4msSA.UIMF";
				FileInfo uimfFile = new FileInfo(fileLocation);

				using (DataWriter uimfWriter = new DataWriter(uimfFile.FullName))
				{
					uimfWriter.CreateBinCentricTables();
				}
			}
			catch (Exception)
			{
				
				
			}
			


		}

		[Test]
		public void TestEncodeDecodeFunctionality()
		{
			int scanLc = 183;
			int scanIms = 217;
			int numImsScans = 360;

			int calculatedIndex = (scanLc * numImsScans) + scanIms;

			int calculatedScanLc = calculatedIndex / numImsScans;
			int calculatedScanIms = calculatedIndex % numImsScans;

			Assert.AreEqual(calculatedScanLc, scanLc);
			Assert.AreEqual(calculatedScanIms, scanIms);
		}
	}
}
