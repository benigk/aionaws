import type { NextPage } from "next";
import styles from "../../styles/MapHome.module.css";
import homestyles from "../../styles/Home.module.css"
import { useEffect, useRef, useState } from "react";
import mapboxgl, { Map } from "mapbox-gl";
import data from "../../public/countries.geo.json";
import Link from "next/link";

mapboxgl.accessToken = "pk.eyJ1Ijoic3BlbmNlcnNjaGFmZXIiLCJhIjoiY2t6Y205cnhsMDFpODMwcTJvMWZ5c3I2biJ9.m_UveaEJUue1b3NfX_hidg";

const options = [
  {
    name: "Publications",
    description: "Estimated total publications",
    property: "account",
    stops: [
      [0, "#ffffff"],
      [1, "#f4bfb6"],
      [5, "#f1a8a5"],
      [10, "#ee8f9a"],
      [50, "#ec739b"],
      [100, "#dd5ca8"],
      [250, "#c44cc0"],
      [500, "#9f43d7"],
      [1000, "#6e40e6"],
      [3000, "#8e40e6"],
      [5000, "#9740e6"],
    ],
  },
];

const MapHome: NextPage = () => {
  const mapContainerRef = useRef(null);
  const [map, setMap] = useState<null | Map>(null);
  const [active, setActive] = useState(options[0]);
  const [lng, setLng] = useState(0);
  const [lat, setLat] = useState(0);
  const [zoom, setZoom] = useState(1);

  useEffect(() => {
    console.log(zoom);
  }, [zoom])

  useEffect(() => {
    const map: Map = new mapboxgl.Map({
      // @ts-ignore
      container: mapContainerRef.current,
      style: "mapbox://styles/mapbox/light-v10?optimize=true",
      center: [lng, lat],
      zoom: zoom,
    });

    map.on("load", () => {
      map.addSource("countries", {
        type: "geojson",
        // @ts-ignore
        data: data,
      });


      map.setLayoutProperty("country-label", "text-field", [
        "format",
        ["get", "name_en"],
        { "font-scale": 1.2 },
        "\n",
        {},
        ["get", "name"],
        {
          "font-scale": 0.8,
          "text-font": [
            "literal",
            ["DIN Offc Pro Italic", "Arial Unicode MS Regular"],
          ],
        },
      ]);

      map.addLayer(
        {
          id: "countries",
          type: "fill",
          source: "countries",
        },
        "country-label",
      );

      map.setPaintProperty("countries", "fill-color", {
        property: active.property,
        stops: active.stops,
      });

      setMap(map);
    });

    return () => map.remove();
  }, []);

  useEffect(() => {
    if (!map) return; // wait for map to initialize
    map.on("move", () => {
      setLng(parseFloat(map.getCenter().lng.toFixed(4)));
      setLat(parseFloat(map.getCenter().lat.toFixed(4)));
      setZoom(parseFloat(map.getZoom().toFixed(2)));
    });
  }, []);

  useEffect(() => {
    paint();
  }, [active]);


  const paint = () => {
    if (map) {
      map.setPaintProperty("countries", "fill-color", {
        property: active.property,
        stops: active.stops,
      });
    }
  };

  return (
    <>
      <div className={homestyles.headerBackground}>
        <Link href="/">
          Home
        </Link>
      </div>
      <div className={styles.container}>
        {/*<main className={styles.main}>*/}
        <div className={styles.sidebar}>
          Longitude: {lng} | Latitude: {lat} | Zoom: {zoom}
        </div>
        <div ref={mapContainerRef} className={styles.mapContainerRef} />
        {/*</main>*/}
      </div>
    </>
  );
}
  ;

export default MapHome;
