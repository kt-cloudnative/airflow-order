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
                , WORKFLOW_NAME='PP_CSNG647B',WORKFLOW_ID='cfe94bb4c1bf424fb85fa510117e670b', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG647B-0.0.prd-tz.2.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 8, 11, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('cfe94bb4c1bf424fb85fa510117e670b')

    csng647bJob_vol = []
    csng647bJob_volMnt = []
    csng647bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng647bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng647bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csng647bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csng647bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng647bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '26215385400c4b69bddd51fbe79a6d06',
        'volumes': csng647bJob_vol,
        'volume_mounts': csng647bJob_volMnt,
        'env_from':csng647bJob_env,
        'task_id':'csng647bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.57',
        'arguments':["--job.name=csng647bJob", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('cfe94bb4c1bf424fb85fa510117e670b')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng647bJob,
        Complete
    ]) 

    # authCheck >> csng647bJob >> Complete
    workflow








