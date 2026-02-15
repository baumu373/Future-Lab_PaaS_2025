// config.js
// IPアドレスが変わったら、ここだけを書き換えればOKです
const CONFIG = {
    // 自分のPCのIPアドレス
    API_BASE_URL: "http://192.168.0.102:8090",      // Goバックエンド
    KEYCLOAK_URL: "http://192.168.0.102:8080",      // Keycloak
    KEYCLOAK_REALM: "future-lab",
    KEYCLOAK_CLIENT_ID: "future-lab-frontend"
};
