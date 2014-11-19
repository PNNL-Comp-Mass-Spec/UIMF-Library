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

        /// <summary>
        /// k
        /// </summary>
        private double K;

        /// <summary>
        /// t0
        /// </summary>
        private double T0;

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
        /// <remarks>
        /// mass = (k * (t-t0))^2
        /// </remarks>
        public MzCalibrator(double k, double t0)
        {
            this.K = k;
            this.T0 = t0;
        }

        #endregion

        #region Public Properties

        /// <summary>
        /// Gets the description.
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
        public double k
        {
            get
            {
                return this.K;
            }

            set
            {
                this.K = value;
            }
        }

        /// <summary>
        /// Gets or sets the t 0.
        /// </summary>
        public double t0
        {
            get
            {
                return this.T0;
            }

            set
            {
                this.T0 = value;
            }
        }

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
        public int MZtoTOF(double mz)
        {
            double r = Math.Sqrt(mz);
            return (int)(((r / this.K) + this.T0) + .5); // .5 for rounding
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
        public double TOFtoMZ(double TOFValue)
        {
            double r = this.K * (TOFValue - this.T0);
            return r * r;
        }

        #endregion
    }
}
