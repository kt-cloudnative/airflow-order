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
                , WORKFLOW_NAME='PP_CBOA123B',WORKFLOW_ID='c2b4bdc6abf3434b908146f3198bd132', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOA123B-0.0.prd-tz.6.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 29, 12, 23, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('c2b4bdc6abf3434b908146f3198bd132')

    cboa123bJob_vol = []
    cboa123bJob_volMnt = []
    cboa123bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cboa123bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cboa123bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    cboa123bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cboa123bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cboa123bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '3c43d4b864c1453f9d508099b6ad623b',
        'volumes': cboa123bJob_vol,
        'volume_mounts': cboa123bJob_volMnt,
        'env_from':cboa123bJob_env,
        'task_id':'cboa123bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.3',
        'arguments':["--job.name=cboa123bJob", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('c2b4bdc6abf3434b908146f3198bd132')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cboa123bJob,
        Complete
    ]) 

    # authCheck >> cboa123bJob >> Complete
    workflow








