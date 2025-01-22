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
                , WORKFLOW_NAME='PP_CSNG216B',WORKFLOW_ID='105cc471479f407cb8c8085f4a8b3362', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG216B-0.0.prd-tz.6.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 29, 11, 19, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('105cc471479f407cb8c8085f4a8b3362')

    csng216bJob_vol = []
    csng216bJob_volMnt = []
    csng216bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng216bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng216bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csng216bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csng216bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng216bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'bef46d99b84f4e48bd3c499d7aeaaa1f',
        'volumes': csng216bJob_vol,
        'volume_mounts': csng216bJob_volMnt,
        'env_from':csng216bJob_env,
        'task_id':'csng216bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.3',
        'arguments':["--job.name=csng216bJob", "endTranDate=20240430", "empNo=91332365","requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('105cc471479f407cb8c8085f4a8b3362')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng216bJob,
        Complete
    ]) 

    # authCheck >> csng216bJob >> Complete
    workflow








