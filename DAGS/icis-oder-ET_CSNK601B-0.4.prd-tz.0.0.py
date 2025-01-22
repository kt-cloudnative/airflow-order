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
                , WORKFLOW_NAME='ET_CSNK601B',WORKFLOW_ID='1ea474259fa145618bf2bea19fc2ea8d', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-ET_CSNK601B-0.4.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 8, 25, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('1ea474259fa145618bf2bea19fc2ea8d')

    csnk601bJob_vol = []
    csnk601bJob_volMnt = []
    csnk601bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csnk601bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csnk601bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnk601bJob_env.extend([getICISConfigMap('icis-oder-etcterr-batch-mng-configmap'), getICISSecret('icis-oder-etcterr-batch-mng-secret'), getICISConfigMap('icis-oder-etcterr-batch-configmap'), getICISSecret('icis-oder-etcterr-batch-secret')])
    csnk601bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnk601bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '607cddf0f07f4decb49cb8f9dab94a4b',
        'volumes': csnk601bJob_vol,
        'volume_mounts': csnk601bJob_volMnt,
        'env_from':csnk601bJob_env,
        'task_id':'csnk601bJob',
        'image':'/icis/icis-oder-etcterr-batch:0.4.1.62',
        'arguments':["--job.name=csnk601bJob", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('1ea474259fa145618bf2bea19fc2ea8d')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnk601bJob,
        Complete
    ]) 

    # authCheck >> csnk601bJob >> Complete
    workflow








