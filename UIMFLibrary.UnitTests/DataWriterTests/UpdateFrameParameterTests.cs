using System;
using System.IO;
using System.Reflection;
using NUnit.Framework;

namespace UIMFLibrary.UnitTests.DataWriterTests
{
    /// <summary>
    /// Update frame parameter tests
    /// </summary>
    [TestFixture]
    public class UpdateFrameParameterTests
    {
        [Test]
        [Category("PNL_Domain")]
        public void UpdateCalibrationCoefficients()
        {
            var sourceFile = new FileInfo(FileRefs.LegacyDemultiplexedFile1);
            if (!sourceFile.Exists)
                Assert.Fail("Test file not found: " + sourceFile.FullName);

            if (sourceFile.Directory == null)
            {
                Assert.Fail("Unable to get the full path to the directory for: " + sourceFile.FullName);
            }

            var targetFolder = new DirectoryInfo(Path.Combine(sourceFile.Directory.FullName, "UIMFLibrary_Temp"));

            if (!targetFolder.Exists)
                targetFolder.Create();

            var targetFilePath = Path.Combine(targetFolder.FullName, sourceFile.Name);

            Console.WriteLine("Copying file " + sourceFile.Name + " to " + targetFilePath);

            sourceFile.CopyTo(targetFilePath, true);

            UpdateCalibrationCoefficients(targetFilePath, 0.3476655, 0.03313236);

            Console.WriteLine("Updated calibration coefficients in " + targetFilePath);
        }

        public void UpdateCalibrationCoefficients(string uimfPath, double slope, double intercept)
        {
            Console.WriteLine("Opening file " + uimfPath);

            var executingAssembly = System.Reflection.Assembly.GetExecutingAssembly();
            using (var writer = new DataWriter(uimfPath, executingAssembly))
            {
                Console.WriteLine("Calling UpdateAllCalibrationCoefficients");

                writer.UpdateAllCalibrationCoefficients(slope, intercept);

                Console.WriteLine("Updated {0} to have slope {1} and intercept {2}", uimfPath, slope, intercept);
            }
        }
    }
}