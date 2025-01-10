import type { OwnerSearched } from '../../../lib/types'
import { CameraStats } from '@/components/charts/RadarChart';
import styles from './owner-card.module.css'
import { OwnerStats } from './OwnerStats';
import { PhotoDisplay } from './PhotoDisplay';

interface OwnerProps{
    owner: OwnerSearched
}

export function OnwerCard({ owner }: OwnerProps){
    return(
        <div className={styles.card}>
            <div className={styles.topRow}>
                <div className={styles.userInfo}>
                    <img
                        src={owner.avatar_url}
                        alt={owner.username}
                        className={styles.avatar}
                    />
                    <OwnerStats owner={owner} />
                </div>
                <div className={styles.chartSection}>
                    <CameraStats cameraStats={owner.camera_details}/>
                </div>
            </div>
            <div className={styles.bottomoRow}>
            <div className={styles.photoContainer}>
                <PhotoDisplay photo={{
                    url: owner.best_photo_url,
                    views: owner.most_viewed_photo_views,
                    comments: owner.most_viewed_photo_comments,
                }}
                rank={owner.rank}/>
                </div>
            </div>
        </div>
    );
}
