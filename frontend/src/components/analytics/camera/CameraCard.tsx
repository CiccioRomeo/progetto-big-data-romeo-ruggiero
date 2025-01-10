import { Camera, Image } from 'lucide-react';
import { formatNumber } from '@/components/maps/clusters/utils';
import styles from './camera-card.module.css';
import { TopBrandAndModels } from '@/lib/types';

interface CameraCardProps {
    camera: TopBrandAndModels;
}

export function CameraCard( { camera }: CameraCardProps ){
    return (
        <div className={styles.card}>
            <div className={styles.header}>
                <div className={styles.brand}>
                <span className={styles.rank}>{camera.rank}°</span>
                    <Camera className={styles.icon} />
                    <h2 className={styles.brandName}>{camera.brand}</h2> 
                </div>
                <div className={styles.stats}>
                    <Image className={styles.countIcon} />
                    <span className={styles.photoCount}>
                        {formatNumber(camera.total_count)}
                    </span>
                </div>
            </div>
            {camera.models_list.map(model => 
                <h2 className={styles.modelName}>#{model[1]}: {model[0]}</h2>
            )}
        </div>
    );
}
