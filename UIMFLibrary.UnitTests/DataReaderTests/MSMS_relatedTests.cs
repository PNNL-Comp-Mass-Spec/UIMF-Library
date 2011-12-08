using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace UIMFLibrary.UnitTests.DataReaderTests
{
    public class MSMS_relatedTests
    {

        [Test]
        public void containsMSMSData_test1()
        {
			using (DataReader reader = new DataReader(new FileInfo(FileRefs.uimfStandardFile1)))
			{
				Assert.AreEqual(false, reader.HasMSMSData());
			}
        }

        //TODO:  need a UIMF standard file that contains MSMS info
        [Test]
        public void containsMSMSData_test2()
        {
			using (DataReader reader = new DataReader(new FileInfo(FileRefs.uimfStandardFile1)))
			{
				Assert.AreEqual(false, reader.HasMSMSData());
			}
        }

    }
}
