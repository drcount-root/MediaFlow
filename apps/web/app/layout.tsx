import type { Metadata } from "next";
import Link from "next/link";
import { UploadCloud, Video } from "lucide-react";
import "./globals.css";

export const metadata: Metadata = {
  title: "MediaFlow",
  description: "Upload, process, and stream adaptive HLS video."
};

export default function RootLayout({
  children
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>
        <header className="app-header">
          <Link href="/" className="brand" aria-label="MediaFlow videos">
            <span className="brand-mark">
              <Video size={18} />
            </span>
            <span>MediaFlow</span>
          </Link>
          <nav className="nav-actions" aria-label="Primary navigation">
            <Link href="/" className="nav-link">
              Videos
            </Link>
            <Link href="/upload" className="primary-link">
              <UploadCloud size={16} />
              Upload
            </Link>
          </nav>
        </header>
        <main className="app-main">{children}</main>
      </body>
    </html>
  );
}

