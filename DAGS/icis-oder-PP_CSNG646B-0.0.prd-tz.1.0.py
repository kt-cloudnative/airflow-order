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
                , WORKFLOW_NAME='PP_CSNG646B',WORKFLOW_ID='e273f34e97b049c996984c0a9952c5d5', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG646B-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 8, 12, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('e273f34e97b049c996984c0a9952c5d5')

    csng646bJob_vol = []
    csng646bJob_volMnt = []
    csng646bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng646bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng646bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csng646bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csng646bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng646bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '9b46f2c435b74ba8834ab524ea8988d2',
        'volumes': csng646bJob_vol,
        'volume_mounts': csng646bJob_volMnt,
        'env_from':csng646bJob_env,
        'task_id':'csng646bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.57',
        'arguments':["--job.name=csng646bJob", "procDay=20240628", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('e273f34e97b049c996984c0a9952c5d5')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng646bJob,
        Complete
    ]) 

    # authCheck >> csng646bJob >> Complete
    workflow








