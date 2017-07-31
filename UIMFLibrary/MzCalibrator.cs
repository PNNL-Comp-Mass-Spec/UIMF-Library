using System;

namespace UIMFLibrary
{

    /// <summary>
    /// The m/z calibrator.
    /// </summary>
    /// <remarks>
    /// Calibrate TOF to m/z according to formula: mass = (k * (t-t0))^2
    /// </remarks>
    public class MzCalibrator
    {
        #region Fields

        private readonly double binWidth;

        private double TenthsOfNanoSecondsPerBin
        {
            get { return this.binWidth * 10; }
        }

        #endregion

        #region Constructors and Destructors

        /// <summary>
        /// Initializes a new instance of the <see cref="MzCalibrator"/> class.
        /// </summary>
        /// <param name="k">
        /// k
        /// </param>
        /// <param name="t0">
        /// t0
        /// </param>
        /// <param name="binWidthNs">
        /// bin width, in nanoseconds
        /// </param>
        /// <remarks>
        /// mass = (k * (t-t0))^2
        /// </remarks>
        public MzCalibrator(double k, double t0, double binWidthNs = 1)
        {
            this.K = k;
            this.T0 = t0;
            this.binWidth = binWidthNs;
        }

        #endregion

        #region Public Properties

        /// <summary>
        /// Returns the calibration equation
        /// </summary>
        public string Description
        {
            get
            {
                return "mz = (k*(t-t0))^2";
            }
        }

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
            return (int)Math.Round(((r / this.K) + this.T0));
        }

        /// <summary>
        /// Convert TOF value to m/z
        /// </summary>
        /// <param name="TOFValue">
        /// The tof value
        /// </param>
        /// <returns>
        /// m/z<see cref="double"/>.
        /// </returns>
        public virtual double TOFtoMZ(double TOFValue)
        {
            var r = this.K * (TOFValue - this.T0);
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
            return this.TOFtoBin(this.MZtoTOF(mz));
        }

        /// <summary>
        /// Convert bin to m/z
        /// </summary>
        /// <param name="bin">bin number</param>
        /// <returns>m/z</returns>
        public double BinToMZ(double bin)
        {
            return this.TOFtoMZ(this.BinToTOF(bin));
        }

        /// <summary>
        /// Convert from a bin number to a TOF value
        /// </summary>
        /// <param name="bin"></param>
        /// <returns></returns>
        public double BinToTOF(double bin)
        {
            return bin * this.TenthsOfNanoSecondsPerBin;
        }

        /// <summary>
        /// Convert from a TOF value to bin number
        /// </summary>
        /// <param name="TOF"></param>
        /// <returns></returns>
        public double TOFtoBin(double TOF)
        {
            return TOF / this.TenthsOfNanoSecondsPerBin;
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
        /// <param name="k">
        /// k
        /// </param>
        /// <param name="t0">
        /// t0
        /// </param>
        /// <param name="binWidthNs">
        /// bin width, in nanoseconds
        /// </param>
        /// <remarks>
        /// mass = (k * (t-t0))^2
        /// </remarks>
        public MzCalibratorFtms(double k, double t0, double binWidthNs = 1) : base(k, t0, binWidthNs)
        {
        }

        /// <summary>
        /// Convert m/z to TOF value
        /// </summary>
        /// <param name="mz">
        /// mz
        /// </param>
        /// <param name="factor">
        /// The scaling factor
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
        /// The tof value
        /// </param>
        /// <param name="factor">
        /// The scaling factor
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
