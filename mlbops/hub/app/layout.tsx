import type { Metadata } from "next";
import Script from "next/script";
import { Inter, JetBrains_Mono, Space_Grotesk } from "next/font/google";
import { HubTopBar } from "@/components/HubTopBar";
import { NavSidebar } from "@/components/NavSidebar";
import "./globals.css";

const inter = Inter({
  variable: "--font-inter",
  subsets: ["latin"],
});

const spaceGrotesk = Space_Grotesk({
  variable: "--font-space-grotesk",
  subsets: ["latin"],
});

const jetbrainsMono = JetBrains_Mono({
  variable: "--font-jetbrains-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "Mallitalytics Hub",
  description: "MLB intelligence terminal and content hub",
};

const themeInitScript = `(function(){try{var k="mlbops-theme",l="malliops-theme";var t=localStorage.getItem(k);if(t!=="light"&&t!=="dark"){var o=localStorage.getItem(l);if(o==="light"||o==="dark"){localStorage.setItem(k,o);try{localStorage.removeItem(l)}catch(e){}t=o}}if(t==="light"||t==="dark")document.documentElement.setAttribute("data-theme",t);else document.documentElement.setAttribute("data-theme","dark");}catch(e){document.documentElement.setAttribute("data-theme","dark");}})();`;

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html
      lang="en"
      suppressHydrationWarning
      className={`${inter.variable} ${spaceGrotesk.variable} ${jetbrainsMono.variable} h-full antialiased`}
    >
      <head>
        <link
          rel="stylesheet"
          href="https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@24,400,0,0&display=swap"
        />
      </head>
      <body className="h-full bg-background text-foreground relative">
        <div className="hub-grid-bg" aria-hidden />
        <Script
          id="mlbops-theme-init"
          strategy="beforeInteractive"
          dangerouslySetInnerHTML={{ __html: themeInitScript }}
        />
        <NavSidebar />
        <div className="flex flex-col min-h-screen lg:min-h-screen lg:ml-[176px]">
          <HubTopBar />
          <main className="flex-1 min-h-0 min-w-0 overflow-y-auto">{children}</main>
        </div>
      </body>
    </html>
  );
}
