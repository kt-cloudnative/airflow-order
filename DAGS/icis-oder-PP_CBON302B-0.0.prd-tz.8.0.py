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
                , WORKFLOW_NAME='PP_CBON302B',WORKFLOW_ID='5848a07ac6eb4c1b9e4635c4634b36cc', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBON302B-0.0.prd-tz.8.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 13, 40, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('5848a07ac6eb4c1b9e4635c4634b36cc')

    cbon302bJob_vol = []
    cbon302bJob_volMnt = []
    cbon302bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbon302bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbon302bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbon302bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cbon302bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbon302bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '3ab1b0ad21ab47198ce3d1a557c49278',
        'volumes': cbon302bJob_vol,
        'volume_mounts': cbon302bJob_volMnt,
        'env_from':cbon302bJob_env,
        'task_id':'cbon302bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=cbon302bJob", 
"requestDate=${YYYYMMDDHHMISSSSS}",
"endTranDate=20241129"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('5848a07ac6eb4c1b9e4635c4634b36cc')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbon302bJob,
        Complete
    ]) 

    # authCheck >> cbon302bJob >> Complete
    workflow








