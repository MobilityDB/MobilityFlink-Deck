import React, { useState, useEffect, useMemo, useCallback, useRef } from "react";
import ReactDOM from "react-dom";
import DeckGL from "@deck.gl/react";
import { StaticMap } from "react-map-gl";
import { FlyToInterpolator } from "@deck.gl/core";
import { TripsLayer } from "@deck.gl/geo-layers";
import { ScatterplotLayer } from "@deck.gl/layers";
import FPSStats from "react-fps-stats";

const CONFIG = {
  mapStyle: "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
  wsUrl: "ws://localhost:8082",
  view: { longitude: -74.27146, latitude: 40.6079, zoom: 12, pitch: 45, bearing: 0 },
  colors: { 
    rt: [23, 184, 190, 180], 
    hist: [255, 165, 0, 180],
    marker: [255, 255, 255, 255],
    markerStroke: [0, 0, 0, 255],
    histMarker: [255, 165, 0, 255],
    histMarkerStroke: [255, 140, 0, 255]
  },
  maxWpts: 200,
  trail: 100,
  recentThreshold: 60000,
  lag: 300,
  followZoom: 16,
  followMs: 500,
  markerRadius: 5,
  markerLineWidth: 1,
  tripLineWidth: 2
};

const toMillis = t => (t = Number(t)) < 1e12 ? t * 1000 : t;

function StatsPanel({ vehicles, now }) {
  const [blink, setBlink] = useState(true);
  useEffect(() => {
    const timer = setInterval(() => setBlink(b => !b), 1000);
    return () => clearInterval(timer);
  }, []);
  
  let points = 0, active = 0;
  for (const v of Object.values(vehicles.rt)) {
    if (v.wpts && v.wpts.length) {
      points += v.wpts.length;
      active++;
    }
  }
  
  return (
    <div style={{
      position: "absolute", bottom: 20, left: 20,
      background: "rgba(30,30,30,0.9)", padding: 15,
      borderRadius: 8, color: "#fff", fontFamily: "monospace",
      fontSize: 14, minWidth: 200, zIndex: 3
    }}>
      <div style={{ marginBottom: 8, display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <span style={{ fontSize: 16, fontWeight: "bold", color: "#17b8be" }}>📊 Stats</span>
        <div style={{
          display: "flex", alignItems: "center", gap: 4,
          background: "#ff0000", padding: "2px 8px",
          borderRadius: 10, fontSize: 11
        }}>
          <div style={{ width: 6, height: 6, background: "white", borderRadius: "50%", opacity: blink ? 1 : 0.3 }} />
          <span style={{ fontWeight: "bold" }}>LIVE</span>
        </div>
      </div>
      <div>📍 Points: {points}</div>
      <div>🚗 Active: {active}</div>
    </div>
  );
}

export default function App({ mapStyle = CONFIG.mapStyle }) {
  const [vehicles, setVehicles] = useState({ rt: {}, hist: {} });
  const [view, setView] = useState(CONFIG.view);
  const [replay, setReplay] = useState(false);
  const [followId, setFollowId] = useState(null);
  const [now, setNow] = useState(Date.now());
  const [showStats, setShowStats] = useState(true);
  
  const epoch = useRef(Date.now());
  const histEpoch = useRef(null);
  const replayStart = useRef(null);
  const queue = useRef([]);
  const lastBatch = useRef(0);

  useEffect(() => {
    let animId;
    const animate = () => {
      const currentTime = Date.now();
      setNow(currentTime);
      
      if (queue.current.length > 0 && currentTime - lastBatch.current > 16) {
        const batch = queue.current.splice(0, 100);
        const updates = { rt: {}, hist: {} };
        
        for (const msg of batch) {
          const { vehicleID, timestamp, longitude, latitude } = msg;
          const id = String(vehicleID);
          let ts = toMillis(timestamp);
          if (ts > currentTime) ts = currentTime - 5;
          
          const isRecent = (currentTime - ts) < CONFIG.recentThreshold;
          if (!replay && !isRecent) continue;
          if (replay && isRecent) continue;
          
          const type = replay ? 'hist' : 'rt';
          
          if (type === 'hist' && !histEpoch.current) {
            histEpoch.current = ts;
            replayStart.current = currentTime;
          }
          
          const point = { coordinates: [longitude, latitude], timestamp: ts };
          
          if (!updates[type][id]) {
            updates[type][id] = [];
          }
          updates[type][id].push(point);
        }
        
        setVehicles(prev => {
          const next = { rt: { ...prev.rt }, hist: { ...prev.hist } };
          
          for (const type of ['rt', 'hist']) {
            for (const [id, points] of Object.entries(updates[type])) {
              const existing = next[type][id] || { wpts: [] };
              let combined = [...existing.wpts, ...points];
              
              if (combined.length > CONFIG.maxWpts) {
                combined = combined.slice(-CONFIG.maxWpts);
              }
              
              next[type][id] = { wpts: combined };
            }
          }
          return next;
        });
        
        lastBatch.current = currentTime;
      }
      
      animId = requestAnimationFrame(animate);
    };
    animate();
    return () => cancelAnimationFrame(animId);
  }, [replay]);

  useEffect(() => {
    let ws;
    try {
      ws = new WebSocket(CONFIG.wsUrl);
      ws.onmessage = event => {
        try {
          const msg = JSON.parse(event.data);
          if (msg.type === "UPDATE" && msg.payload) {
            queue.current.push(msg.payload);
          }
        } catch (e) {}
      };
    } catch (e) {}
    return () => { if (ws) ws.close(); };
  }, []);

  useEffect(() => {
    if (!replay) {
      setVehicles(prev => ({ ...prev, hist: {} }));
      histEpoch.current = null;
      replayStart.current = null;
    }
  }, [replay]);

  const processed = useMemo(() => {
    const trips = { rt: [], hist: [] };
    const markers = [];
    const list = [];
    
    const currentSec = Math.max(0, (now - epoch.current - CONFIG.lag) / 1000);
    const historySec = replay && replayStart.current && histEpoch.current 
      ? (now - replayStart.current) / 1000 : 0;
    
    for (const type of ['rt', 'hist']) {
      for (const [id, vehicle] of Object.entries(vehicles[type])) {
        list.push({ id, type });
        const wpts = vehicle.wpts || [];
        
        if (wpts.length >= 2) {
          const ref = type === 'hist' ? histEpoch.current : epoch.current;
          const timestamps = wpts.map(p => (p.timestamp - ref) / 1000);
          
          trips[type].push({
            vehicleID: id,
            path: wpts.map(p => p.coordinates),
            timestamps: timestamps
          });
          
          const animTime = type === 'hist' ? historySec : currentSec;
          let pos = null;
          
          if (animTime >= timestamps[timestamps.length - 1]) {
            pos = wpts[wpts.length - 1].coordinates;
          } else if (animTime <= timestamps[0]) {
            pos = wpts[0].coordinates;
          } else {
            for (let i = 0; i < timestamps.length - 1; i++) {
              if (animTime >= timestamps[i] && animTime <= timestamps[i + 1]) {
                const r = (animTime - timestamps[i]) / (timestamps[i + 1] - timestamps[i]);
                const p1 = wpts[i].coordinates, p2 = wpts[i + 1].coordinates;
                pos = [p1[0] + (p2[0] - p1[0]) * r, p1[1] + (p2[1] - p1[1]) * r];
                break;
              }
            }
          }
          
          if (pos) {
            markers.push({
              vehicleID: id,
              coordinates: pos,
              type: type,
              timestamp: wpts[wpts.length - 1].timestamp
            });
          }
        } else if (wpts.length === 1) {
          markers.push({
            vehicleID: id,
            coordinates: wpts[0].coordinates,
            type: type,
            timestamp: wpts[0].timestamp
          });
        }
      }
    }
    
    list.sort((a, b) => a.type === b.type ? parseInt(a.id) - parseInt(b.id) : a.type === 'rt' ? -1 : 1);
    return { trips, markers, list, currentSec, historySec };
  }, [vehicles, now, replay]);


  const flyTo = useCallback((keyId, zoom = CONFIG.followZoom) => {
    if (!keyId) return;
    const [id, type] = keyId.startsWith('H-') ? [keyId.slice(2), 'hist'] : [keyId, 'rt'];
    const marker = processed.markers.find(m => m.vehicleID === id && m.type === type);
    
    if (marker) {
      setView(prev => ({
        ...prev,
        longitude: marker.coordinates[0],
        latitude: marker.coordinates[1],
        zoom: Math.max(prev.zoom, zoom),
        transitionDuration: CONFIG.followMs,
        transitionInterpolator: new FlyToInterpolator()
      }));
    }
  }, [processed.markers]);

  useEffect(() => {
    if (!followId) return;
    flyTo(followId);
    const interval = setInterval(() => flyTo(followId), CONFIG.followMs);
    return () => clearInterval(interval);
  }, [followId, flyTo]);

  const layers = useMemo(() => {
    const layerList = [];
    const tripProps = {
      getPath: d => d.path,
      getTimestamps: d => d.timestamps,
      opacity: 0.8,
      widthMinPixels: CONFIG.tripLineWidth,
      rounded: true,
      trailLength: CONFIG.trail,
      pickable: false,
      parameters: { depthTest: false }
    };
    
    if (processed.trips.hist.length > 0) {
      layerList.push(new TripsLayer({
        id: "trips-hist",
        data: processed.trips.hist,
        getColor: CONFIG.colors.hist,
        currentTime: processed.historySec,
        ...tripProps
      }));
    }
    
    if (processed.trips.rt.length > 0) {
      layerList.push(new TripsLayer({
        id: "trips-rt",
        data: processed.trips.rt,
        getColor: CONFIG.colors.rt,
        currentTime: processed.currentSec,
        ...tripProps
      }));
    }
    
    if (processed.markers.length > 0) {
      layerList.push(new ScatterplotLayer({
        id: "markers",
        data: processed.markers,
        getPosition: d => d.coordinates,
        getFillColor: d => d.type === 'hist' ? CONFIG.colors.histMarker : CONFIG.colors.marker,
        stroked: true,
        getLineColor: d => d.type === 'hist' ? CONFIG.colors.histMarkerStroke : CONFIG.colors.markerStroke,
        lineWidthUnits: 'pixels',
        getLineWidth: CONFIG.markerLineWidth,
        getRadius: CONFIG.markerRadius,
        radiusUnits: 'pixels',
        pickable: true,
        autoHighlight: false,
        parameters: { depthTest: false }
      }));
    }
    
    return layerList;
  }, [processed]);

  const boxStyle = { position: "absolute", background: "rgba(30,30,30,.85)", padding: 10, borderRadius: 5, color: "#eee", font: "12px sans-serif" };

  return (
    <div style={{ position: "relative", width: "100vw", height: "100vh" }}>
      <DeckGL
        layers={layers}
        viewState={view}
        onViewStateChange={e => setView(e.viewState)}
        controller
        getTooltip={({ object }) => {
          if (!object) return null;
          const date = object.timestamp ? new Date(object.timestamp) : null;
          const dateStr = date ? date.toLocaleDateString() : "N/A";
          const timeStr = date ? date.toLocaleTimeString() : "N/A";
          return `Vehicle: ${object.vehicleID}\nDate: ${dateStr}\nTime: ${timeStr}`;
        }}
        onClick={({ object }) => {
          if (object) {
            const key = (object.type === 'hist' ? 'H-' : '') + object.vehicleID;
            setFollowId(key);
            flyTo(key);
          }
        }}
      >
        <StaticMap reuseMaps mapStyle={mapStyle} preventStyleDiffing />
      </DeckGL>

      <div style={{ ...boxStyle, top: 10, right: 10, zIndex: 3 }}>
        <label style={{ cursor: "pointer" }}>
          <input type="checkbox" checked={replay} onChange={e => setReplay(e.target.checked)} />
          {" Historical only"}
        </label>
        <label style={{ cursor: "pointer", marginLeft: 15 }}>
          <input type="checkbox" checked={showStats} onChange={e => setShowStats(e.target.checked)} />
          {" Show stats"}
        </label>
        {followId && (
          <div style={{ marginTop: 8 }}>
            <div style={{ fontSize: 12 }}>Following: <b>{followId}</b></div>
            {(() => {
              const [id, type] = followId.startsWith('H-') ? [followId.slice(2), 'hist'] : [followId, 'rt'];
              const vehicle = vehicles[type] && vehicles[type][id];
              if (vehicle && vehicle.wpts && vehicle.wpts.length > 0) {
                const lastPoint = vehicle.wpts[vehicle.wpts.length - 1];
                if (lastPoint && lastPoint.timestamp) {
                  const date = new Date(lastPoint.timestamp);
                  return (
                    <div style={{ fontSize: 11, marginTop: 2, color: "#17b8be" }}>
                      {date.toLocaleDateString()} - {date.toLocaleTimeString()}
                    </div>
                  );
                }
              }
              return <div style={{ fontSize: 10, marginTop: 2, color: "#666" }}>No timestamp data</div>;
            })()}
          </div>
        )}
        <div style={{ marginTop: 8, fontSize: 11, color: "#888" }}>
          {replay ? "Showing: Old data only (>60s ago)" : "Showing: Recent data only (<60s)"}
        </div>
      </div>

      <div style={{ ...boxStyle, right: 10, top: 120, width: 220, maxHeight: "65vh", overflowY: "auto", zIndex: 2 }}>
        <h4 style={{ margin: "0 0 10px 0", borderBottom: "1px solid #666", paddingBottom: 5 }}>
          Active ({processed.list.length})
        </h4>
        {processed.list.length === 0 ? (
          <div style={{ fontStyle: "italic", color: "#888" }}>No vehicles yet...</div>
        ) : (
          processed.list.slice(0, 500).map(v => {
            const key = (v.type === 'hist' ? 'H-' : '') + v.id;
            const isFollowing = followId === key;
            
            return (
              <div
                key={key}
                style={{
                  cursor: "pointer",
                  padding: "4px 0",
                  borderBottom: "1px solid #444",
                  color: isFollowing ? "#FF0000" : (v.type === 'hist' ? "#FFA500" : "#eee"),
                  background: "transparent",
                  fontWeight: isFollowing ? 700 : 400
                }}
                onClick={() => { setFollowId(key); flyTo(key); }}
                onDoubleClick={e => { e.stopPropagation(); setFollowId(isFollowing ? null : key); }}
              >
                {v.type === 'hist' && "[H] "}ID: {v.id}{isFollowing && " • following"}
              </div>
            );
          })
        )}
      </div>

      {showStats && <StatsPanel vehicles={vehicles} now={now} />}

      <div style={{ position: "absolute", top: 10, left: 10, zIndex: 3 }}>
        <FPSStats />
      </div>
    </div>
  );
}

export function renderToDOM(container) {
  ReactDOM.render(<App />, container);
}