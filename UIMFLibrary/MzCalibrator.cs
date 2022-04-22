using System;

namespace UIMFLibrary
{
    /// <summary>
    /// m/z calibrator
    /// </summary>
    /// <remarks>
    /// Calibrate TOF to m/z according to formula: mass = (k * (t-t0))^2
    /// </remarks>
    public class MzCalibrator
    {
        #region Fields

        private readonly double binWidth;

        private double TenthsOfNanoSecondsPerBin => binWidth * 10;

        #endregion

        #region Constructor

        /// <summary>
        /// Initializes a new instance of the <see cref="MzCalibrator"/> class.
        /// </summary>
        /// <remarks>
        /// mass = (k * (t-t0))^2
        /// </remarks>
        /// <param name="k">
        /// k
        /// </param>
        /// <param name="t0">
        /// t0
        /// </param>
        /// <param name="binWidthNs">
        /// bin width, in nanoseconds
        /// </param>
        public MzCalibrator(double k, double t0, double binWidthNs = 1)
        {
            K = k;
            T0 = t0;
            binWidth = binWidthNs;
        }

        #endregion

        #region Public Properties

        /// <summary>
        /// Returns the calibration equation
        /// </summary>
        public string Description => "mz = (k*(t-t0))^2";

        /// <summary>
        /// Gets or sets the k.
        /// </summary>
        public double K { get; set; }

        /// <summary>
        /// Gets or sets the t 0.
        /// </summary>
        public double T0 { get; set; }

        #endregion

        #region Public Methods and Operators

        /// <summary>
        /// Convert m/z to TOF value
        /// </summary>
        /// <param name="mz">
        /// mz
        /// </param>
        /// <returns>
        /// TOF value<see cref="int"/>.
        /// </returns>
        public virtual int MZtoTOF(double mz)
        {
            var r = Math.Sqrt(mz);
            return (int)Math.Round(r / K + T0);
        }

        /// <summary>
        /// Convert TOF value to m/z
        /// </summary>
        /// <param name="TOFValue">
        /// TOF value
        /// </param>
        /// <returns>
        /// m/z<see cref="double"/>.
        /// </returns>
        public virtual double TOFtoMZ(double TOFValue)
        {
            var r = K * (TOFValue - T0);
            return Math.Pow(r, 2);
        }

        /// <summary>
        /// Convert m/z to bin number
        /// </summary>
        /// <param name="mz">m/z</param>
        /// <returns>bin number</returns>
        public double MZtoBin(double mz)
        {
            // TODO: Add TOFCorrectionTime?
            return TOFtoBin(MZtoTOF(mz));
        }

        /// <summary>
        /// Convert bin to m/z
        /// </summary>
        /// <param name="bin">bin number</param>
        /// <returns>m/z</returns>
        public double BinToMZ(double bin)
        {
            return TOFtoMZ(BinToTOF(bin));
        }

        /// <summary>
        /// Convert from a bin number to a TOF value
        /// </summary>
        /// <param name="bin"></param>
        public double BinToTOF(double bin)
        {
            return bin * TenthsOfNanoSecondsPerBin;
        }

        /// <summary>
        /// Convert from a TOF value to bin number
        /// </summary>
        /// <param name="TOF"></param>
        public double TOFtoBin(double TOF)
        {
            return TOF / TenthsOfNanoSecondsPerBin;
        }

        #endregion
    }

    /// <summary>
    /// Extends MzCalibrator to add support for a scaling factor
    /// </summary>
    public class MzCalibratorFtms : MzCalibrator
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="MzCalibratorFtms"/> class.
        /// </summary>
        /// <remarks>
        /// mass = (k * (t-t0))^2
        /// </remarks>
        /// <param name="k">
        /// k
        /// </param>
        /// <param name="t0">
        /// t0
        /// </param>
        /// <param name="binWidthNs">
        /// bin width, in nanoseconds
        /// </param>
        public MzCalibratorFtms(double k, double t0, double binWidthNs = 1) : base(k, t0, binWidthNs)
        {
        }

        /// <summary>
        /// Convert m/z to TOF value
        /// </summary>
        /// <param name="mz">
        /// m/z
        /// </param>
        /// <param name="factor">
        /// Scaling factor
        /// </param>
        /// <returns>
        /// TOF value<see cref="int"/>.
        /// </returns>
        public int MZtoTOF(double mz, double factor)
        {
           var tof = base.MZtoTOF(mz) * factor;
           return (int)tof;
        }

        /// <summary>
        /// Convert TOF value to m/z
        /// </summary>
        /// <param name="tofValue">
        /// TOF value
        /// </param>
        /// <param name="factor">
        /// Scaling factor
        /// </param>
        /// <returns>
        /// m/z<see cref="double"/>.
        /// </returns>
        public double ToftoMz(double tofValue, double factor)
        {
           var mz = TOFtoMZ(tofValue);
            mz /= factor;
            return Math.Pow(mz, 2);
        }
    }
}
