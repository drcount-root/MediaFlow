import { UploadForm } from "./upload-form";

export default function UploadPage() {
  return (
    <>
      <div className="page-title">
        <div>
          <h1>Upload</h1>
          <p>Submit an MP4 and MediaFlow will queue it for HLS processing.</p>
        </div>
      </div>
      <div className="panel">
        <UploadForm />
      </div>
    </>
  );
}

