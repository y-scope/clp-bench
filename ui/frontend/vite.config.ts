import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'

// https://vitejs.dev/config/
export default defineConfig(({ command, mode }) => {
    const env = loadEnv(mode, '../', '');
    let proxy
    if (command === "serve") {
        proxy = {
            "/api": {
                target: `http://${env.VITE_BACKEND_HOST}:${env.VITE_BACKEND_PORT}`,
                changeOrigin: true,
                secure: false,
            }
        };
    }
    return {
        base: env.VITE_FRONTEND_BASE_PATH,
        envDir: '../',
        plugins: [react()],
        server: {
            host: env.VITE_FRONTEND_HOST,
            port: JSON.parse(env.VITE_FRONTEND_PORT),
            proxy: proxy,
        },
    }
})