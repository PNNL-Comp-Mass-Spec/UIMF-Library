using System;
using System.Collections.Generic;
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
            UIMFLibrary.DataReader reader = new DataReader();
            reader.OpenUIMF(FileRefs.uimfStandardFile1);

            Assert.AreEqual(false, reader.hasMSMSData());
            reader.CloseUIMF();
        }

        //TODO:  need a UIMF standard file that contains MSMS info
        [Test]
        public void containsMSMSData_test2()
        {
            UIMFLibrary.DataReader reader = new DataReader();
            reader.OpenUIMF(FileRefs.uimfStandardFile1);

            Assert.AreEqual(false, reader.hasMSMSData());
            reader.CloseUIMF();
        }


    }
}
