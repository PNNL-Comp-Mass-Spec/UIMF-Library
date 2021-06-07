using System;
using NUnit.Framework;

namespace UIMFLibrary.UnitTests.DataReaderTests
{
    [TestFixture]
    public class DynamicConversionTests
    {
        [Test]
        [TestCase("Test data", typeof(string), true)]
        [TestCase("3", typeof(byte), true)]
        [TestCase("3", typeof(short), true)]
        [TestCase("3", typeof(int), true)]
        [TestCase("3", typeof(double), true)]
        [TestCase("3", typeof(float), true)]
        [TestCase("3.0", typeof(byte), true)]
        [TestCase("3.0", typeof(short), true)]
        [TestCase("3.0", typeof(int), true)]
        [TestCase("3.0", typeof(double), true)]
        [TestCase("3.0", typeof(float), true)]
        [TestCase("3.5", typeof(byte), false)]
        [TestCase("3.5", typeof(short), false)]
        [TestCase("3.5", typeof(int), false)]
        [TestCase("3.5", typeof(double), true)]
        [TestCase("3.5", typeof(float), true)]
        [TestCase("not a number", typeof(byte), false)]
        [TestCase("not a number", typeof(short), false)]
        [TestCase("not a number", typeof(int), false)]
        [TestCase("not a number", typeof(double), false)]
        [TestCase("not a number", typeof(float), false)]
        public void TestConvertStringToDynamic(string value, Type targetType, bool convertible)
        {
            var result = FrameParamUtilities.ConvertStringToDynamic(targetType, value);

            if (result == null)
            {
                // Conversion failed
                if (convertible)
                    Assert.Fail("Conversion of {0} to {1} should have succeeded", value, targetType);

                Console.WriteLine("As expected, could not convert {0} to {1}", value, targetType);
                return;
            }

            if (targetType == typeof(string))
            {
                Assert.AreEqual(value, (string)result);
                return;
            }

            Console.WriteLine("Converted {0} to {1}", value, targetType);
        }

        [Test]
        [TestCase("3", true)]
        [TestCase("3.234", true)]
        [TestCase("-3.234", true)]
        [TestCase("53E3", true)]
        [TestCase("53E+3", true)]
        [TestCase("not a number", false)]
        public void TestConvertDynamicToDouble(string valueText, bool convertible)
        {
            dynamic valueAsDynamic = valueText;

            var success = FrameParamUtilities.ConvertDynamicToDouble(valueAsDynamic, out double value);

            if (success)
            {
                Console.WriteLine("Converted {0} to {1}", valueText, value);
            }

            if (convertible)
                Assert.IsTrue(success, "Conversion to double failed for {0}", valueText);
            else
            {
                Assert.IsFalse(success, "Conversion to double unexpectedly succeeded for {0}", valueText);

                Console.WriteLine("As expected, could not convert {0} to a double", valueText);
            }
        }

        [Test]
        [TestCase("3", true)]
        [TestCase("3.234", false)]
        [TestCase("-3.234", false)]
        [TestCase("6983224", true)]
        [TestCase("53E3", false)]
        [TestCase("53E+3", false)]
        [TestCase("not a number", false)]
        public void TestConvertDynamicToInt32(string valueText, bool convertible)
        {
            dynamic valueAsDynamic = valueText;

            var success = FrameParamUtilities.ConvertDynamicToInt32(valueAsDynamic, out int value);

            if (success)
            {
                Console.WriteLine("Converted {0} to {1}", valueText, value);
            }

            if (convertible)
                Assert.IsTrue(success, "Conversion to int failed for {0}", valueText);
            else
            {
                Assert.IsFalse(success, "Conversion to int unexpectedly succeeded for {0}", valueText);

                Console.WriteLine("As expected, could not convert {0} to a int", valueText);
            }
        }

        [Test]
        public void TestConvertFrameParametersToDictionary()
        {
            var frameParameters = GetExampleFrameParameters();
            var frameParams = FrameParamUtilities.ConvertFrameParameters(frameParameters);

            var success = frameParams.TryGetValue(FrameParamKeyType.StartTimeMinutes, out var startTime);
            Assert.IsTrue(success, "Start time could not be retrieved");
            Assert.AreEqual(frameParameters.StartTime, startTime, 0.00001);

            success = frameParams.TryGetValue(FrameParamKeyType.Scans, out var scans);
            Assert.IsTrue(success, "Scans could not be retrieved");
            Assert.AreEqual(frameParameters.Scans, scans);

            success = frameParams.TryGetValue(FrameParamKeyType.CalibrationSlope, out var calibrationSlope);
            Assert.IsTrue(success, "CalibrationSlope could not be retrieved");
            Assert.AreEqual(frameParameters.CalibrationSlope, calibrationSlope, 0.00001);

            success = frameParams.TryGetValue(FrameParamKeyType.FrameType, out var frameType);
            Assert.IsTrue(success, "FrameType could not be retrieved");
            Assert.AreEqual((int)frameParameters.FrameType, frameType);
        }

        [Test]
        public void TestConverFrameParametersToClass()
        {
            var frameParameters = GetExampleFrameParameters();
            var frameParamsByType = FrameParamUtilities.ConvertFrameParameters(frameParameters);
            var frameParams = FrameParamUtilities.ConvertDynamicParamsToFrameParams(frameParamsByType);

            var startTime = frameParams.GetValueDouble(FrameParamKeyType.StartTimeMinutes);
            Assert.AreEqual(frameParameters.StartTime, startTime, 0.00001);

            var startTimeDynamicInt = frameParams.GetValue(FrameParamKeyType.StartTimeMinutes, 0);
            Assert.AreEqual(frameParameters.StartTime, startTimeDynamicInt, 0.00001);

            var startTimeDynamicDouble = frameParams.GetValue(FrameParamKeyType.StartTimeMinutes, 0.0);
            Assert.AreEqual(frameParameters.StartTime, startTimeDynamicDouble, 0.00001);

            var startTimeDynamic= frameParams.GetValue(FrameParamKeyType.StartTimeMinutes);
            Assert.AreEqual(frameParameters.StartTime, startTimeDynamic, 0.00001);

            var scans = frameParams.GetValueInt32(FrameParamKeyType.Scans);
            Assert.AreEqual(frameParameters.Scans, scans);

            var scansDynamicInt = frameParams.GetValue(FrameParamKeyType.Scans, 0);
            Assert.AreEqual(frameParameters.Scans, scansDynamicInt);

            var scansDynamicDouble = frameParams.GetValue(FrameParamKeyType.Scans, 0.0);
            Assert.AreEqual(frameParameters.Scans, scansDynamicDouble, 0.00001);

            var calibrationSlope = frameParams.GetValueDouble(FrameParamKeyType.CalibrationSlope);
            Assert.AreEqual(frameParameters.CalibrationSlope, calibrationSlope, 0.00001);

            var calibrationSlopeDynamicInt = frameParams.GetValue(FrameParamKeyType.CalibrationSlope, 0);
            Assert.AreEqual(frameParameters.CalibrationSlope, calibrationSlopeDynamicInt);

            var calibrationSlopeDynamicDouble = frameParams.GetValue(FrameParamKeyType.CalibrationSlope, 0.0);
            Assert.AreEqual(frameParameters.CalibrationSlope, calibrationSlopeDynamicDouble, 0.00001);

            // Remove the slope and try again
            frameParams.Values.Remove(FrameParamKeyType.CalibrationSlope);
            calibrationSlope = frameParams.GetValueDouble(FrameParamKeyType.CalibrationSlope, 3.4);
            Assert.AreEqual(3.4, calibrationSlope, 0.00001);

            calibrationSlopeDynamicInt = frameParams.GetValue(FrameParamKeyType.CalibrationSlope, 2);
            Assert.AreEqual(2, calibrationSlopeDynamicInt);

            calibrationSlopeDynamicDouble = frameParams.GetValue(FrameParamKeyType.CalibrationSlope, 2.5);
            Assert.AreEqual(2.5, calibrationSlopeDynamicDouble, 0.00001);

            var frameType = frameParams.GetValueInt32(FrameParamKeyType.FrameType);
            Assert.AreEqual((int)frameParameters.FrameType, frameType);

            var frameTypeDynamic = frameParams.GetValue(FrameParamKeyType.FrameType, 0);
            Assert.AreEqual((int)frameParameters.FrameType, frameTypeDynamic);

            var frameTypeDynamicDouble = frameParams.GetValue(FrameParamKeyType.FrameType, 0.0);
            Assert.AreEqual((double)frameParameters.FrameType, frameTypeDynamicDouble, 0.00001);

            var imfProfile = frameParams.GetValue(FrameParamKeyType.MultiplexingEncodingSequence);
            Assert.AreEqual(imfProfile, "Seq");

            var imfProfileWithDefault = frameParams.GetValue(FrameParamKeyType.MultiplexingEncodingSequence, "");
            Assert.AreEqual(imfProfileWithDefault, "Seq");

            var imfProfileString = frameParams.GetValueString(FrameParamKeyType.MultiplexingEncodingSequence);
            Assert.AreEqual(imfProfileString, "Seq");
        }

        [Test]
        public void TestConvertGlobalParametersToDictionary()
        {
            var globalParameters = GetExampleGlobalParameters();
            var globalParams = GlobalParamUtilities.ConvertGlobalParameters(globalParameters);

            var success = globalParams.TryGetValue(GlobalParamKeyType.InstrumentName, out var instName);
            Assert.IsTrue(success, "Instrument name could not be retrieved");
            Assert.AreEqual(globalParameters.InstrumentName, instName);

            success = globalParams.TryGetValue(GlobalParamKeyType.NumFrames, out var numFrames);
            Assert.IsTrue(success, "NumFrames could not be retrieved");
            Assert.AreEqual(globalParameters.NumFrames, numFrames);

            success = globalParams.TryGetValue(GlobalParamKeyType.BinWidth, out var binWidth);
            Assert.IsTrue(success, "BinWidth could not be retrieved");
            Assert.AreEqual(globalParameters.BinWidth, binWidth, 0.00001);
        }

        [Test]
        public void TestConvertGlobalParametersToClass()
        {
            var globalParameters = GetExampleGlobalParameters();
            var globalParamsByType = GlobalParamUtilities.ConvertGlobalParameters(globalParameters);

            var globalParams = GlobalParamUtilities.ConvertDynamicParamsToGlobalParams(globalParamsByType);

            var instName = globalParams.GetValueString(GlobalParamKeyType.InstrumentName);
            Assert.AreEqual(globalParameters.InstrumentName, instName);

            var instNameDynamic = globalParams.GetValue(GlobalParamKeyType.InstrumentName, "");
            Assert.AreEqual(globalParameters.InstrumentName, instNameDynamic);

            var instNameDynamicWithDefault = globalParams.GetValue(GlobalParamKeyType.InstrumentName);
            Assert.AreEqual(globalParameters.InstrumentName, instNameDynamicWithDefault);

            // Remove the instrument name then test again
            globalParams.Values.Remove(GlobalParamKeyType.InstrumentName);

            instName = globalParams.GetValueString(GlobalParamKeyType.InstrumentName);
            Assert.AreEqual("", instName);

            instName = globalParams.GetValueString(GlobalParamKeyType.InstrumentName, "Undefined");
            Assert.AreEqual("Undefined", instName);

            instNameDynamic = globalParams.GetValue(GlobalParamKeyType.InstrumentName, "Undefined");
            Assert.AreEqual("Undefined", instNameDynamic);

            instNameDynamicWithDefault = globalParams.GetValue(GlobalParamKeyType.InstrumentName);
            Assert.AreEqual("", instNameDynamicWithDefault);

            var numFrames = globalParams.GetValueInt32(GlobalParamKeyType.NumFrames);
            Assert.AreEqual(globalParameters.NumFrames, numFrames);

            var numFramesDynamic = globalParams.GetValue(GlobalParamKeyType.NumFrames, 0);
            Assert.AreEqual(globalParameters.NumFrames, numFramesDynamic);

            var numFramesDynamicDouble = globalParams.GetValue(GlobalParamKeyType.NumFrames, 0.0);
            Assert.AreEqual(globalParameters.NumFrames, numFramesDynamicDouble, 0.00001);

            var binWidth = globalParams.GetValueInt32(GlobalParamKeyType.BinWidth);
            Assert.AreEqual(globalParameters.BinWidth, binWidth);

            var binWidthDynamic = globalParams.GetValue(GlobalParamKeyType.BinWidth, 0);
            Assert.AreEqual(globalParameters.BinWidth, binWidthDynamic);

            var binWidthDynamicDouble = globalParams.GetValue(GlobalParamKeyType.BinWidth, 0.0);
            Assert.AreEqual(globalParameters.BinWidth, binWidthDynamicDouble, 0.00001);
        }

#pragma warning disable 612, 618

        private FrameParameters GetExampleFrameParameters()
        {
            var frameParameters = new FrameParameters
            {
                FrameNum = 2,
                StartTime = 266.95178,
                Duration = 0,
                Accumulations = 18,
                FrameType = UIMFData.FrameType.MS1,
                Scans = 360,
                IMFProfile = "Seq",
                TOFLosses = 61,
                AverageTOFLength = 162555.56,
                CalibrationSlope = 0.347341992892325,
                CalibrationIntercept = 0.0279005798339844,
                a2 = 0,
                b2 = 0,
                c2 = 0,
                d2 = 0,
                e2 = 0,
                f2 = 0,
                Temperature = 135.0,
                voltHVRack1 = 26.9,
                voltHVRack2 = 3822.0,
                voltHVRack3 = 1796.0,
                PressureFront = 3.734,
                PressureBack = 3.796,
                MPBitOrder = 0
            };

            return frameParameters;
        }

        private GlobalParameters GetExampleGlobalParameters()
        {
            var globalParameters = new GlobalParameters
            {
                InstrumentName = "Test",
                DateStarted = "08/09/2017 8:00:00 am",
                NumFrames = 10,
                TimeOffset = 10000,
                BinWidth = 1.0,
                Bins = 138000,
                TOFCorrectionTime = 0.0f,
                FrameDataBlobVersion = 0.1f,      // Legacy parameter that was always 0.1
                ScanDataBlobVersion = 0.1f,       // Legacy parameter that was always 0.1
                TOFIntensityType = "ADC",
                DatasetType = "",
                Prescan_TOFPulses = 50,
                Prescan_Accumulations = 20,
                Prescan_TICThreshold = 300,
                Prescan_Continuous = false,
                Prescan_Profile = "SA_4ms.txt"
            };

            return globalParameters;
        }

#pragma warning restore 612, 618

    }
}
