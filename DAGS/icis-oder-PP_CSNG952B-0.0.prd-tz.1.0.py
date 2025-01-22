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
                , WORKFLOW_NAME='PP_CSNG952B',WORKFLOW_ID='3bc97a91199d4097bb8f7b56ba673960', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG952B-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 14, 20, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('3bc97a91199d4097bb8f7b56ba673960')

    csng952bJob_vol = []
    csng952bJob_volMnt = []
    csng952bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng952bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng952bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csng952bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    csng952bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng952bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng952bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng952bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'c8f5c986540840c5baf618c1b7f301ed',
        'volumes': csng952bJob_vol,
        'volume_mounts': csng952bJob_volMnt,
        'env_from':csng952bJob_env,
        'task_id':'csng952bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=csng952bJob", "endTranDate=20241129", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('3bc97a91199d4097bb8f7b56ba673960')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng952bJob,
        Complete
    ]) 

    # authCheck >> csng952bJob >> Complete
    workflow








