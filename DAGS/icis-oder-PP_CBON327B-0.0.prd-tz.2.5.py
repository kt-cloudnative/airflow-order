from datetime import datetime, timedelta
from kubernetes.client import models as k8s
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.helpers import chain, cross_downstream
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
import sys
sys.path.append('/opt/bitnami/airflow/dags/git_sa-common')

from icis_common import *
COMMON = ICISCmmn(DOMAIN='oder',ENV='prd-tz', NAMESPACE='t-order'
                , WORKFLOW_NAME='PP_CBON327B',WORKFLOW_ID='2c3d941d0ca146aab7ea6b6230e9e0da', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBON327B-0.0.prd-tz.2.5'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 29, 11, 56, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('2c3d941d0ca146aab7ea6b6230e9e0da')

    cbon327bJob_vol = []
    cbon327bJob_volMnt = []
    cbon327bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbon327bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbon327bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    cbon327bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbon327bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbon327bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'ca21cfaa09f3406db64acb110f8741a5',
        'volumes': cbon327bJob_vol,
        'volume_mounts': cbon327bJob_volMnt,
        'env_from':cbon327bJob_env,
        'task_id':'cbon327bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.3',
        'arguments':["--job.name=cbon327bJob", "endTranDate=20240430","ver="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('2c3d941d0ca146aab7ea6b6230e9e0da')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbon327bJob,
        Complete
    ]) 

    # authCheck >> cbon327bJob >> Complete
    workflow








