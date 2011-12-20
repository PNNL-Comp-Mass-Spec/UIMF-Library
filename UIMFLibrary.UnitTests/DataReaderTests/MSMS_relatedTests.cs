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
            using (DataReader reader = new DataReader(FileRefs.uimfStandardFile1))
            {
                Assert.AreEqual(false, reader.HasMSMSData());
            }
        }


        [Test]
        public void containsMSMSData_test2()
        {
            using (var reader = new DataReader(FileRefs.uimfStandardFile1))
            {
                Assert.AreEqual(false, reader.HasMSMSData());
            }
        }

        [Test]
        public void containsMSMSDataTest3()
        {
            using (var reader = new DataReader(FileRefs.uimfContainingMSMSData1))
            {
                Assert.AreEqual(true, reader.HasMSMSData());

            }

        }

        [Test]
        public void GetMSLevelTest1()
        {
            using (var reader = new DataReader(FileRefs.uimfContainingMSMSData1))
            {
                GlobalParameters gp = reader.GetGlobalParameters();


                int checkSum = 0;

                for (int frame = 1; frame <= gp.NumFrames; frame++)
                {
                    checkSum += frame * reader.GetMSLevelForFrame(frame);
                    
                   // Console.WriteLine(frame  + "\t" + reader.GetMSLevelForFrame(frame));
                }

                Assert.AreEqual(204, checkSum);

            }

            //TODO: finish and assert something


        }


    }
}
