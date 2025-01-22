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
                , WORKFLOW_NAME='WC_CBOD601B',WORKFLOW_ID='318f45c30834458ba76f8def6be2847e', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-WC_CBOD601B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2023, 11, 1, 11, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('318f45c30834458ba76f8def6be2847e')

    cbod601bJob_vol = []
    cbod601bJob_volMnt = []
    cbod601bJob_env = [getICISConfigMap('icis-oder-wrlincomn-batch-configmap'), getICISConfigMap('icis-oder-wrlincomn-batch-configmap2'), getICISSecret('icis-oder-wrlincomn-batch-secret')]
    cbod601bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbod601bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbod601bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'daaa3ea4993f4ef7bed8cb6814142e21',
        'volumes': cbod601bJob_vol,
        'volume_mounts': cbod601bJob_volMnt,
        'env_from':cbod601bJob_env,
        'task_id':'cbod601bJob',
        'image':'/icis/icis-oder-wrlincomn-batch:0.4.1.17',
        'arguments':["--job.name=cbod601bJob", "date="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('318f45c30834458ba76f8def6be2847e')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbod601bJob,
        Complete
    ]) 

    # authCheck >> cbod601bJob >> Complete
    workflow








