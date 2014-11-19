// --------------------------------------------------------------------------------------------------------------------
// <summary>
//   Defines the frame type info.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace UIMFLibrary
{
	/// <summary>
	/// Class for tracking frame index of each frame number defined in a .UIMF file
	/// </summary>
	internal class FrameSetContainer
	{
		#region Constructors and Destructors

		/// <summary>
		/// Constructor
		/// </summary>
		/// <param name="numFramesInFile">
		/// Number of frames in the file.
		/// </param>
		public FrameSetContainer(int numFramesInFile)
		{
			this.NumFrames = 0;
			this.FrameIndexes = new int[numFramesInFile + 1];
		}

		#endregion

		#region Public Properties

		/// <summary>
		/// Mapping between frame number and frame index
		/// </summary>
		public int[] FrameIndexes { get; private set; }

		/// <summary>
		/// Gets the num frames.
		/// </summary>
		public int NumFrames { get; private set; }

		#endregion

		#region Public Methods and Operators

		/// <summary>
		/// Add a frame.
		/// </summary>
		/// <param name="frameNumber">
		/// Frame number.
		/// </param>
		public void AddFrame(int frameNumber)
		{
			this.FrameIndexes[frameNumber] = this.NumFrames;
			this.NumFrames++;
		}

		#endregion
	}
}