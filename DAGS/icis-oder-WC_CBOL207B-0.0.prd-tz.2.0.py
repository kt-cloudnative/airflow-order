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
                , WORKFLOW_NAME='WC_CBOL207B',WORKFLOW_ID='19a3054e39cd4444b5385c6d02a2443f', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-WC_CBOL207B-0.0.prd-tz.2.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 1, 13, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('19a3054e39cd4444b5385c6d02a2443f')

    cbol207bJob_vol = []
    cbol207bJob_volMnt = []
    cbol207bJob_env = [getICISConfigMap('icis-oder-wrlincomn-batch-configmap'), getICISConfigMap('icis-oder-wrlincomn-batch-configmap2'), getICISSecret('icis-oder-wrlincomn-batch-secret')]
    cbol207bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbol207bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbol207bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'b39faed6136e4e4f8725c38df613083b',
        'volumes': cbol207bJob_vol,
        'volume_mounts': cbol207bJob_volMnt,
        'env_from':cbol207bJob_env,
        'task_id':'cbol207bJob',
        'image':'/icis/icis-oder-wrlincomn-batch:0.4.1.4',
        'arguments':["--job.name=cbol207bJob", "endTranDate=20240723", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('19a3054e39cd4444b5385c6d02a2443f')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbol207bJob,
        Complete
    ]) 

    # authCheck >> cbol207bJob >> Complete
    workflow








