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
                , WORKFLOW_NAME='PP_CSNG193B',WORKFLOW_ID='885775db8da44638a154c9706805ed1c', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG193B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 3, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('885775db8da44638a154c9706805ed1c')

    csng193bJob_vol = []
    csng193bJob_volMnt = []
    csng193bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng193bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng193bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng193bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'b6de647800564a4e8dc6a1dfc77adfea',
        'volumes': csng193bJob_vol,
        'volume_mounts': csng193bJob_volMnt,
        'env_from':csng193bJob_env,
        'task_id':'csng193bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.94',
        'arguments':["--job.name=csng193bJob", "requestDate="+str(datetime.now()), "workDate=${YYYYMMDD}", "workEmpNo=p91366966"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('885775db8da44638a154c9706805ed1c')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng193bJob,
        Complete
    ]) 

    # authCheck >> csng193bJob >> Complete
    workflow








