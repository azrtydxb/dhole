/// <reference types="vite/client" />

interface ImportMetaEnv {
  /** Base URL of the dhole control plane; defaults to the serving origin. */
  readonly VITE_DHOLE_API_URL?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
