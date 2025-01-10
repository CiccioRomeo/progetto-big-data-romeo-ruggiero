import { useEffect, useState } from 'react';
import DeckGL from '@deck.gl/react';
import { Map } from 'react-map-gl/maplibre';
import 'maplibre-gl/dist/maplibre-gl.css';
import { INITIAL_VIEW_STATE, MAP_STYLE } from '../constants';
import { createHexagonLayer } from './layers';
import { lightingEffect } from '../lighting';
import { getTooltip } from './tooltip';
import type { Coordinate } from '@/lib/types';
import styles from '../MapView.module.css';

interface MapViewProp{
  coordinates: Coordinate[]
}

export function MapView( { coordinates }: MapViewProp ) {
  const [elevationScale, setElevationScale] = useState(0); // Elevation iniziale

  useEffect(() => {
    const interval = setInterval(() => {
      setElevationScale(prev => {
        if (prev < 1) {
          return prev + 0.05;
        } else {
          clearInterval(interval); 
          return 1;
        }
      });
    }, 60);
    return () => clearInterval(interval);
  }, []);

  const layers = [createHexagonLayer(coordinates, elevationScale)];

  return (
    <div className={styles.container}>
      <DeckGL
        initialViewState={INITIAL_VIEW_STATE}
        controller={true}
        layers={layers}
        effects={[lightingEffect]}
        getTooltip={getTooltip}
      >
        <Map
          reuseMaps
          mapStyle={MAP_STYLE}
          attributionControl={false}
        />
      </DeckGL>
    </div>
  );
}
