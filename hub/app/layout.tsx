import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import Link from "next/link";
import "./globals.css";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "Mallitalytics Hub",
  description: "MLB content queue and publishing hub",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html
      lang="en"
      className={`${geistSans.variable} ${geistMono.variable} h-full antialiased`}
    >
      <body className="min-h-full flex flex-col bg-[#f7fafc] text-[#1a202c]">
        {/* Top nav */}
        <nav className="bg-[#1a365d] text-white px-6 py-3 flex items-center gap-6 shadow-sm">
          <Link href="/queue" className="font-bold text-lg tracking-tight flex items-center gap-2">
            ⚾ Mallitalytics
          </Link>
          <div className="flex items-center gap-4 text-sm ml-4">
            <Link href="/queue" className="hover:text-blue-200 transition-colors font-medium">
              Queue
            </Link>
            <Link href="/schedule" className="hover:text-blue-200 transition-colors">
              Schedule
            </Link>
            <Link href="/leaderboards" className="hover:text-blue-200 transition-colors">
              Leaderboards
            </Link>
            <Link href="/settings" className="hover:text-blue-200 transition-colors">
              Settings
            </Link>
          </div>
        </nav>

        {/* Main content */}
        <main className="flex-1">{children}</main>
      </body>
    </html>
  );
}
