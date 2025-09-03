#!/usr/bin/env python3

import json
import requests
import sys

def send_batch(vehicles, api_url):
    data = list(vehicles.values())

    r = requests.post(api_url, json=data, timeout=30)
    if r.status_code in [200, 202]:
        result = r.json()
        return result.get('sent', 0)
    else:
        print(f"error: {r.status_code} - {r.text}")
        return 0

def main():
    if len(sys.argv) < 2:
        print("usage: python dataset_loader.py <file.json>")
        sys.exit(1)
    
    filename = sys.argv[1]
    api_url = "http://localhost:5000/ingest/location"
    batch_size = 500 
    
    print(f"reading {filename}...")
    
    with open(filename, 'r') as f:
        first_char = f.read(1)
        f.seek(0)
        
        if first_char == '[':
            flights = json.load(f)
        else:
            flights = []
            for line in f:
                line = line.strip()
                if line and line != ',':
                    try:
                        flights.append(json.loads(line.rstrip(',')))
                    except:
                        continue
    
    print(f"processing {len(flights)} flights...")
    
    vehicles = {}
    total_sent = 0
    batch_count = 0
    
    for i, flight in enumerate(flights):
        vid = flight.get("icao24")
        if not vid:
            continue
            
        if vid not in vehicles:
            vehicles[vid] = {
                "vehicleID": vid,
                "path": [],
                "timestamps": []
            }
        
        lat = flight.get("lat")
        lon = flight.get("lon") 
        ts = flight.get("time")
        
        if lat and lon and ts:
            vehicles[vid]["path"].append([lon, lat])
            vehicles[vid]["timestamps"].append(ts)
        
        if len(vehicles) >= batch_size:
            batch_count += 1
            print(f"sending batch {batch_count} ({len(vehicles)} objects)...")
            sent = send_batch(vehicles, api_url)
            total_sent += sent
            vehicles = {} 
    

    if vehicles:
        batch_count += 1
        print(f"sending final batch {batch_count} ({len(vehicles)} objects)...")
        sent = send_batch(vehicles, api_url)
        total_sent += sent
    
    print(f"done. total points sent: {total_sent}")

if __name__ == "__main__":
    main()