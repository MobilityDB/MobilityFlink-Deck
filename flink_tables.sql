DROP TRIGGER IF EXISTS trigger_insert_vehicle_locations ON vehicle_locations_temporal_insert_view;
DROP VIEW IF EXISTS vehicle_locations_temporal_insert_view;
DROP TABLE IF EXISTS vehicle_locations_temporal CASCADE;


CREATE TABLE vehicle_locations_temporal (
    vehicle_id TEXT NOT NULL,
    timestamp BIGINT NOT NULL,
    tgeogpoint TGEOGPOINT NOT NULL
);



CREATE INDEX IF NOT EXISTS idx_vehicle_locations_temporal_tgeogpoint_gist 
    ON vehicle_locations_temporal USING GIST (tgeogpoint);

CREATE INDEX IF NOT EXISTS idx_vehicle_locations_temporal_vehicle_time 
    ON vehicle_locations_temporal(vehicle_id, timestamp);



CREATE OR REPLACE VIEW vehicle_locations_temporal_insert_view AS
SELECT 
    vehicle_id, 
    timestamp, 
    tgeogpoint::TEXT AS tgeogpoint_text
FROM vehicle_locations_temporal;



CREATE OR REPLACE FUNCTION trg_insert_vehicle_locations()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO vehicle_locations_temporal(vehicle_id, timestamp, tgeogpoint)
    VALUES (NEW.vehicle_id, NEW.timestamp, NEW.tgeogpoint_text::TGEOGPOINT);
    RETURN NULL;
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'Erreur de conversion tgeogpoint : véhicule=%, valeur=%', NEW.vehicle_id, NEW.tgeogpoint_text;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;



CREATE TRIGGER trigger_insert_vehicle_locations
INSTEAD OF INSERT ON vehicle_locations_temporal_insert_view
FOR EACH ROW EXECUTE FUNCTION trg_insert_vehicle_locations();
