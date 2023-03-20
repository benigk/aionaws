import L from "leaflet";

export const initialiseMap = () => {
    console.log('isPageLoaded');
    const mapboxLink = 'https://api.mapbox.com/styles/v1/mapbox/light-v10/tiles/{z}/{x}/{y}?access_token=';
    const accessToken = 'pk.eyJ1Ijoic3BlbmNlcnNjaGFmZXIiLCJhIjoiY2t6Y205cnhsMDFpODMwcTJvMWZ5c3I2biJ9.m_UveaEJUue1b3NfX_hidg';

    const lightmap = L.tileLayer(mapboxLink + accessToken, {
        attribution: '© <a href="https://www.mapbox.com/feedback/">Mapbox</a> © <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>',
        tileSize: 512,
        zoomOffset: -1
    });

    const map = L.map('map', {
        center: [45, 0],
        zoom: 3,
        layers: [lightmap]
    })
    return map;

}
